package meta

import (
	"context"
	"testing"
	"time"

	"kidb/internal/redistest"
)

func TestCatalogSaveLoad(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	ctx := context.Background()
	store := NewCatalogStore(cli, reg)

	def := &TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "uid", Type: ColInt, NotNull: true},
			{Name: "city", Type: ColString},
		},
		PK: "uid",
		Indexes: []IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: IndexEq},
		},
		ExpShards: 1,
	}
	if err := store.Save(ctx, def, 0); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := store.Load(ctx, "users")
	if err != nil || got == nil {
		t.Fatalf("Load: %v, %v", got, err)
	}
	if got.PK != "uid" || got.Ver != 1 || len(got.Indexes) != 1 || got.Indexes[0].Kind != IndexEq {
		t.Fatalf("Load = %+v", got)
	}

	// 版本校验：旧期望版本应 stale
	if err := store.Save(ctx, def, 0); err == nil {
		t.Fatal("Save with stale expectVer must fail")
	}
	if err := store.Save(ctx, def, 1); err != nil {
		t.Fatalf("Save ver=1: %v", err)
	}

	ver, err := store.SchemaVersion(ctx)
	if err != nil || ver != 2 {
		t.Fatalf("SchemaVersion = %d, %v; want 2", ver, err)
	}

	// 不存在的表
	miss, err := store.Load(ctx, "nope")
	if err != nil || miss != nil {
		t.Fatalf("Load missing = %+v, %v", miss, err)
	}
}

func TestLeaseTracker(t *testing.T) {
	l := NewLeaseTracker(time.Second)
	t0 := time.Unix(1000, 0)

	if l.Fresh(t0) {
		t.Fatal("从未校验过快照不得 Fresh")
	}
	if changed := l.Checked(t0, 7); !changed {
		t.Fatal("首次校验应报告 changed")
	}
	if !l.Fresh(t0.Add(500 * time.Millisecond)) {
		t.Fatal("租约窗口内应 Fresh")
	}
	if l.Fresh(t0.Add(2 * time.Second)) {
		t.Fatal("越出租约必须重新校验（docs/06 §6.2 越界必检）")
	}
	if changed := l.Checked(t0.Add(2*time.Second), 7); changed {
		t.Fatal("版本未变不应报告 changed")
	}
	l.Invalidate()
	if l.Fresh(t0.Add(2500 * time.Millisecond)) {
		t.Fatal("Invalidate 后不得 Fresh（stale 强制校验路径）")
	}
}
