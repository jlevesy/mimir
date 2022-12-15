//go:build requires_docker

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/e2e"
	"github.com/grafana/e2e/db"
	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

func TestCompactBlocksContainingNativeHistograms(t *testing.T) {
	const numBlocks = 2

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// inDir is a staging directory for blocks we upload to the bucket.
	inDir := t.TempDir()
	// outDir is a staging directory for blocks we download from the bucket after compaction.
	outDir := t.TempDir()

	bkt, err := s3.NewBucketClient(s3.Config{
		Endpoint:        minio.HTTPEndpoint(),
		Insecure:        true,
		BucketName:      mimirBucketName,
		AccessKeyID:     e2edb.MinioAccessKey,
		SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
	}, "test", log.NewNopLogger())
	require.NoError(t, err)
	bktClient := bucket.NewPrefixedBucketClient(bkt, userID)
	defer func() {
		require.NoError(t, bktClient.Close())
	}()

	// Create a few blocks to compact and upload them to the bucket.
	metas := make([]*metadata.Meta, 0, numBlocks)
	expectedSeries := make([]series, numBlocks)

	for i := 0; i < numBlocks; i++ {
		spec := testutil.BlockSeriesSpec{
			Labels: labels.FromStrings("case", "native_histogram", "i", strconv.Itoa(i)),
			Chunks: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(10, 0, e2e.GenerateTestHistogram(1), nil),
					newSample(20, 0, e2e.GenerateTestHistogram(2), nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(30, 0, e2e.GenerateTestHistogram(3), nil),
					newSample(40, 0, e2e.GenerateTestHistogram(4), nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(50, 0, e2e.GenerateTestHistogram(5), nil),
					newSample(2*time.Hour.Milliseconds()-1, 0, e2e.GenerateTestHistogram(6), nil),
				}),
			},
		}

		// Populate expectedSeries for comparison w/ compactedSeries later.
		var samples []sample
		for _, chk := range spec.Chunks {
			it := chk.Chunk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, h := it.AtHistogram()
				samples = append(samples, sample{t: ts, h: h})
			}
		}
		expectedSeries[i] = series{lbls: spec.Labels, samples: samples}

		meta, err := testutil.GenerateBlockFromSpec(userID, inDir, []*testutil.BlockSeriesSpec{&spec})
		require.NoError(t, err)

		for _, f := range meta.Thanos.Files {
			r, err := os.Open(filepath.Join(inDir, f.RelPath))
			require.NoError(t, err)
			require.NoError(t, bktClient.Upload(context.Background(), filepath.Join(f.RelPath), r))
		}

		metas = append(metas, meta)
	}

	// Start compactor.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), mergeFlags(CommonStorageBackendFlags(), BlocksStorageFlags()))
	require.NoError(t, s.StartAndWaitReady(compactor))

	// Wait for the compactor to run.
	require.NoError(t, compactor.WaitSumMetrics(e2e.Greater(0), "cortex_compactor_runs_completed_total"))

	var blocks []string
	require.NoError(t, bktClient.Iter(context.Background(), "", func(name string) error {
		blocks = append(blocks, filepath.Base(name))
		return nil
	}))

	var compactedSeries []series

	for _, blockID := range blocks {
		require.NoError(t, block.Download(context.Background(), log.NewNopLogger(), bktClient, ulid.MustParseStrict(blockID), filepath.Join(outDir, blockID)))

		chkReader, err := chunks.NewDirReader(filepath.Join(outDir, blockID, block.ChunksDirname), nil)
		require.NoError(t, err)

		ixReader, err := index.NewFileReader(filepath.Join(outDir, blockID, block.IndexFilename))
		require.NoError(t, err)

		all, err := ixReader.Postings(index.AllPostingsKey())
		require.NoError(t, err)

		for p := ixReader.SortedPostings(all); p.Next(); {
			var lbls labels.Labels
			var chks []chunks.Meta

			var samples []sample

			require.NoError(t, ixReader.Series(p.At(), &lbls, &chks))

			for _, c := range chks {
				c.Chunk, err = chkReader.Chunk(c)
				require.NoError(t, err)

				it := c.Chunk.Iterator(nil)
				for {
					valType := it.Next()

					if valType == chunkenc.ValNone {
						break
					} else if valType == chunkenc.ValHistogram {
						ts, h := it.AtHistogram()
						samples = append(samples, sample{
							t: ts,
							h: h,
						})
					} else {
						t.Error("Unexpected chunkenc.ValueType (we're expecting only histograms): " + string(valType))
					}
				}
			}

			compactedSeries = append(compactedSeries, series{
				lbls:    lbls,
				samples: samples,
			})
		}

		require.NoError(t, ixReader.Close())
		require.NoError(t, chkReader.Close())

		// This block ULID should not be the same as any of the pre-compacted ones (otherwise the corresponding block
		//did not get compacted).
		for _, m := range metas {
			require.NotEqual(t, m.ULID.String(), blockID)
		}
	}

	require.Equal(t, expectedSeries, compactedSeries)
}

type series struct {
	lbls    labels.Labels
	samples []sample
}

type sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func newSample(t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) tsdbutil.Sample {
	return sample{t, v, h, fh}
}
func (s sample) T() int64                      { return s.t }
func (s sample) V() float64                    { return s.v }
func (s sample) H() *histogram.Histogram       { return s.h }
func (s sample) FH() *histogram.FloatHistogram { return s.fh }

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}
