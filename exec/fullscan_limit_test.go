package exec

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// TestFullscanLimit 全扫限流通道（docs/07 §7.4）：并发全扫受信号量约束，
// 超限查询排队（而非击穿集群）。
func TestFullscanLimit(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := &meta.TableDef{
		Name: "fs",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "v", Type: meta.ColInt},
		},
		PK: "id",
	}
	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"v": strconv.Itoa(i)}})
		require.NoError(t, err)
	}

	e := New(cli, reg)
	e.SetFullscanLimit(1) // 单槽位

	// 流 A 打开（不排空——占住槽位）
	sa := e.Run(ctx, &Request{Table: tbl, Kind: FullScan})
	row, err := sa.Next() // 首个 fill 获取槽位
	require.NoError(t, err)
	require.NotNil(t, row)

	// 流 B 在槽位满时应阻塞；带超时 ctx 验证排队行为
	bctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	sb := e.Run(bctx, &Request{Table: tbl, Kind: FullScan})
	_, err = sb.Next()
	require.ErrorIs(t, err, context.DeadlineExceeded, "槽位满时全扫必须排队（ctx 超时）")
	sb.Close()
	cancel()

	// A 排空释放 → B 立即可跑
	for {
		_, err := sa.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	sa.Close()
	sb2 := e.Run(ctx, &Request{Table: tbl, Kind: FullScan})
	n := 0
	for {
		_, err := sb2.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		n++
	}
	sb2.Close()
	require.Equal(t, 20, n)
}
