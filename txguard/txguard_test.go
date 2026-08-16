package txguard

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/testutil"
)

func testTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "users",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: ColIntAlias, NotNull: true},
			{Name: "city", Type: meta.ColString},
			{Name: "age", Type: meta.ColInt},
			{Name: "email", Type: meta.ColString},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq, PrefixCopy: true},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange},
			{ID: "uk_email", Columns: []string{"email"}, Kind: meta.IndexUnique},
		},
	}
}

// ColIntAlias 避免测试样板里重复点号（本地别名）。
const ColIntAlias = meta.ColInt

func slotOf(tbl *meta.TableDef, pk string) uint16 {
	return keycodec.Slot(keycodec.RowKey(tbl.Name, pk))
}

// TestWriteRowInvariants 覆盖 docs/12 §12.2 的七项一致性断言主路径：
// 行、等值桶、范围桶、字典序副本、exp、cnt、rcpt（+唯一预约）。
func TestWriteRowInvariants(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := New(cli, reg, nil)
	tbl := testTable()
	p := testutil.NewProbe(t, cli)
	ctx := context.Background()

	slot := slotOf(tbl, "1")
	eq := keycodec.EqBucketKey(tbl.Name, "idx_city", "shanghai", slot, 0)
	rg := keycodec.RangeBucketKey(tbl.Name, "idx_age", slot, 0)
	lex := keycodec.LexBucketKey(tbl.Name, "idx_city", slot, 0)
	exp := keycodec.ExpKey(tbl.Name, slot)
	cnt := keycodec.CntKey(tbl.Name, slot)
	rcpt := keycodec.ReceiptKey(tbl.Name, "1")
	row := keycodec.RowKey(tbl.Name, "1")
	resv := keycodec.UniqueKey(tbl.Name, "uk_email", "a@x.com")

	// 1. 全新插入（带 TTL）
	_, err := g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "1",
		Fields: map[string]string{"city": "shanghai", "age": "30", "email": "a@x.com"},
		TTL:    time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, "shanghai", p.HGet(row, "city"), "行字段")
	require.Equal(t, "1", p.HGet(row, "_ver"), "行 _ver")
	require.Equal(t, "0", p.ZScore(eq, "1"), "等值桶 member")
	require.Equal(t, "30", p.ZScore(rg, "1"), "范围桶 score")
	require.Equal(t, "0", p.ZScore(lex, "shanghai\x001"), "字典序副本 member")
	require.NotEmpty(t, p.ZScore(exp, "1"), "exp 登记")
	require.Equal(t, "1", p.Get(cnt), "cnt")
	require.True(t, p.Exists(rcpt), "TTL 行必须有回执")
	require.Contains(t, p.Get(resv), row, "唯一预约指向行")
	require.Greater(t, p.PTTL(row), int64(0), "行 TTL")

	// 2. 更新：改等值/范围值，换唯一值 → 旧桶撤销、新桶建立、预约换绑
	_, err = g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "1",
		Fields: map[string]string{"city": "beijing", "age": "31", "email": "b@x.com"},
		TTL:    time.Hour,
	})
	require.NoError(t, err)
	require.Empty(t, p.ZScore(eq, "1"), "旧等值桶应撤销")
	require.Equal(t, "0", p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "beijing", slot, 0), "1"))
	require.Equal(t, "31", p.ZScore(rg, "1"))
	require.Empty(t, p.ZScore(lex, "shanghai\x001"))
	require.Equal(t, "0", p.ZScore(lex, "beijing\x001"))
	require.Equal(t, "1", p.Get(cnt), "更新不动计数")
	require.Equal(t, "2", p.HGet(row, "_ver"))
	require.Empty(t, p.Get(resv), "旧预约应释放")
	require.Contains(t, p.Get(keycodec.UniqueKey(tbl.Name, "uk_email", "b@x.com")), row)

	// 3. 唯一冲突：另一行占同值
	_, err = g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "2",
		Fields: map[string]string{"email": "b@x.com"},
	})
	require.ErrorIs(t, err, kidb.ErrDuplicateKey)

	// 4. 撤销 TTL：回执删除、行 PERSIST
	_, err = g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "1",
		Fields: map[string]string{"city": "beijing", "age": "31", "email": "b@x.com"},
	})
	require.NoError(t, err)
	require.False(t, p.Exists(rcpt), "无 TTL 行无回执")
	require.Equal(t, int64(-1), p.PTTL(row), "PERSIST 后无 TTL")
	expScore := p.ZScore(exp, "1")
	expScoreF, _ := strconv.ParseFloat(expScore, 64)
	require.True(t, math.IsInf(expScoreF, 1), "无 TTL 行 exp score 应为 +inf，got %s", expScore)

	// 5. 删除
	ok, err := g.DeleteRow(ctx, tbl, "1")
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, p.Exists(row))
	require.Empty(t, p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "beijing", slot, 0), "1"))
	require.Empty(t, p.ZScore(rg, "1"))
	require.Empty(t, p.ZScore(exp, "1"))
	require.Equal(t, "0", p.Get(cnt))
	require.Empty(t, p.Get(keycodec.UniqueKey(tbl.Name, "uk_email", "b@x.com")), "删除释放预约")

	// 6. 删除不存在/已过期行 → false，无副作用
	ok, err = g.DeleteRow(ctx, tbl, "1")
	require.NoError(t, err)
	require.False(t, ok)
}

// TestResurrection 主键复活：旧行已过期、回执仍在 → 按回执撤销旧索引、
// cnt 不重复 INCR（跨脚本不变式见 write_row.lua 头部）。
func TestResurrection(t *testing.T) {
	cli, reg, m := testutil.New(t)
	g := New(cli, reg, nil)
	tbl := testTable()
	p := testutil.NewProbe(t, cli)
	ctx := context.Background()

	_, err := g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "9",
		Fields: map[string]string{"city": "shanghai", "email": "r@x.com"},
		TTL:    time.Hour,
	})
	require.NoError(t, err)
	// 行物理过期，但回执仍在宽限期内（行 TTL 1h + grace 300s，快进到 1h+100s）。
	// 注意：快进超过宽限期回执也会被回收——生产上 sweeper 早已清扫（docs/07 §7.3）。
	m.FastForward(time.Hour + 100*time.Second)
	require.False(t, p.Exists(keycodec.RowKey(tbl.Name, "9")))

	// 复活：同名主键重新写入
	_, err = g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "9",
		Fields: map[string]string{"city": "hangzhou", "email": "r2@x.com"},
	})
	require.NoError(t, err)

	slot := slotOf(tbl, "9")
	require.Empty(t, p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "shanghai", slot, 0), "9"),
		"复活必须按旧回执撤销旧索引")
	require.Equal(t, "0", p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "hangzhou", slot, 0), "9"))
	require.Equal(t, "1", p.Get(keycodec.CntKey(tbl.Name, slot)), "复活不重复 INCR（docs/05 跨脚本不变式）")
}

// TestCASWriteGuard 调用方 CAS 写语义：期望版本与预读不符 → fail-fast 不重试
// （docs/05 §5.6：预读→提交间的并发竞态才走 stale 整体重试）。
func TestCASWriteGuard(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := New(cli, reg, nil)
	tbl := testTable()
	ctx := context.Background()

	_, err := g.WriteRow(ctx, WriteReq{Table: tbl, PK: "5", Fields: map[string]string{"city": "x"}})
	require.NoError(t, err)

	// 期望 _ver=0（实际已为 1）→ 立即 ErrStaleMetadata，写入不生效
	zero := int64(0)
	_, err = g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "5", Fields: map[string]string{"city": "z"},
		ExpectedOldVer: &zero,
	})
	require.ErrorIs(t, err, kidb.ErrStaleMetadata)
	require.Equal(t, "x", testutil.NewProbe(t, cli).HGet(keycodec.RowKey(tbl.Name, "5"), "city"))

	// 期望与当前一致（_ver=1）→ 正常写入
	one := int64(1)
	res, err := g.WriteRow(ctx, WriteReq{
		Table: tbl, PK: "5", Fields: map[string]string{"city": "y"},
		ExpectedOldVer: &one,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.NewVer)
}
