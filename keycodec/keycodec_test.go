package keycodec

import (
	"strings"
	"testing"
)

// 行 key 与其全部辅助 key 同 slot（docs/03 §3.2 的目标）。
func TestRowColocation(t *testing.T) {
	table, pk := "users", "u_9527"
	rowSlot := Slot(RowKey(table, pk))
	// 行内聚结构：回执。
	if Slot(ReceiptKey(table, pk)) != rowSlot {
		t.Fatal("receipt must colocate with row")
	}
	// slot 内聚结构：登记册/计数器/异步日志使用同一 stag。
	tag := SlotTag(rowSlot)
	for _, k := range []string{
		ExpKey(table, rowSlot),
		ExpShardKey(table, rowSlot, 3),
		CntKey(table, rowSlot),
		AsyncLogKey(table, "idx_a", rowSlot),
	} {
		if !strings.Contains(k, tag) {
			t.Fatalf("%s must carry stag %s", k, tag)
		}
		if Slot(k) != rowSlot {
			t.Fatalf("%s must colocate with row slot %d", k, rowSlot)
		}
	}
}

// 唯一预约 key：同值必同 key 同 slot（docs/05 §5.3）。
func TestUniqueKeyDeterministic(t *testing.T) {
	k1 := UniqueKey("users", "idx_email", "alice@x.com")
	k2 := UniqueKey("users", "idx_email", "alice@x.com")
	if k1 != k2 || Slot(k1) != Slot(k2) {
		t.Fatal("same value must map to same reservation key/slot")
	}
	if k3 := UniqueKey("users", "idx_email", "bob@x.com"); k3 == k1 {
		t.Fatal("different values must not collide (pre-image)")
	}
}

// L4 副本必须落在与源桶不同的 slot 且确定性可寻址（docs/08 §8.4）。
func TestReplicaOffSlot(t *testing.T) {
	src := EqBucketKey("users", "idx_city", "shanghai", 42, 0)
	const stride = 16384 / 9
	seen := map[uint16]bool{Slot(src): true}
	for k := 1; k <= 8; k++ {
		rep := ReplicaKey(src, k)
		want := (uint16(42) + uint16(k*stride)) % NumSlots
		if Slot(rep) != want {
			t.Fatalf("replica %d slot = %d, want %d", k, Slot(rep), want)
		}
		if seen[Slot(rep)] {
			t.Fatalf("replica %d collides with another slot", k)
		}
		seen[Slot(rep)] = true
	}
}
