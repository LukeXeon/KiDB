package exec

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// replicaFake 主/副本读通道分离计数的测试客户端（miniredis 无真实副本，
// 副本通道委托主通道执行，只验证路由分流决策）。
type replicaFake struct {
	kidb.KvClient
	replicaOK bool
	mu        sync.Mutex
	main      int // Pipeline 主通道计数
	rep       int // PipelineReplica 副本通道计数
}

func (f *replicaFake) Capabilities() kidb.Capabilities {
	c := f.KvClient.Capabilities()
	c.ReplicaRead = f.replicaOK
	return c
}

func (f *replicaFake) Pipeline(ctx context.Context, cmds []kidb.Cmd) ([]any, error) {
	f.mu.Lock()
	f.main += len(cmds)
	f.mu.Unlock()
	return f.KvClient.Pipeline(ctx, cmds)
}

func (f *replicaFake) PipelineReplica(ctx context.Context, cmds []kidb.Cmd) ([]any, error) {
	f.mu.Lock()
	f.rep += len(cmds)
	f.mu.Unlock()
	return f.KvClient.Pipeline(ctx, cmds) // miniredis 无副本：委托主通道
}

func (f *replicaFake) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.main, f.rep
}

// TestL3ReplicaRouting L3 副本读分流（docs/08 §8.4 + docs/09 §9.4）：
// 开关开 + 能力在 → 读走 PipelineReplica；能力缺失 → 自动回落主通道（降级不失效）。
func TestL3ReplicaRouting(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	fake := &replicaFake{KvClient: cli, replicaOK: true}
	g := txguard.New(cli, reg, nil)
	tbl := &meta.TableDef{
		Name: "t",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "tag", Type: meta.ColString},
		},
		PK:      "id",
		Indexes: []meta.IndexDef{{ID: "idx_tag", Columns: []string{"tag"}, Kind: meta.IndexEq}},
	}
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"tag": "x"}})
		require.NoError(t, err)
	}

	e := New(fake, reg)
	idx := tbl.Index("idx_tag")
	req := func() *Request {
		return &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"x"},
			Pred: &Predicate{Column: "tag", Eq: []string{"x"}}}
	}

	// 默认关：全走主通道
	rows := drain(t, e.Run(ctx, req()))
	require.Len(t, rows, 5)
	main1, rep1 := fake.counts()
	require.Positive(t, main1)
	require.Zero(t, rep1)

	// 开 + 能力在：走副本通道
	e.SetReplicaRead(true)
	rows = drain(t, e.Run(ctx, req()))
	require.Len(t, rows, 5)
	main2, rep2 := fake.counts()
	require.Equal(t, main1, main2, "开启后主通道不再承担读")
	require.Positive(t, rep2, "副本通道承接读")

	// 能力缺失：自动回落主通道（降级纪律，docs/09 §9.4）
	fake.replicaOK = false
	rows = drain(t, e.Run(ctx, req()))
	require.Len(t, rows, 5)
	main3, rep3 := fake.counts()
	require.Positive(t, main3-main2, "能力缺失回落主通道")
	require.Equal(t, rep2, rep3, "能力缺失不再走副本")
}
