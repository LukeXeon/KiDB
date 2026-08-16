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

// TestEscapeValueDigest 摘要桶规则（docs/03 §3.2）：
// 超长/含结构字符的值走 xxhash64 摘要（~x 前缀），同值必同 key。
func TestEscapeValueDigest(t *testing.T) {
	// 常规值：URL escape 直通
	if got := EscapeValue("shanghai"); got != "shanghai" {
		t.Fatalf("EscapeValue = %q", got)
	}
	// 含冒号：摘要
	a := EscapeValue("has:colon")
	if !HasDigestPrefix(a) {
		t.Fatalf("含冒号值应摘要，got %q", a)
	}
	// 超长：摘要
	long := make([]byte, 200)
	for i := range long {
		long[i] = 'x'
	}
	if !HasDigestPrefix(EscapeValue(string(long))) {
		t.Fatal("超长值应摘要")
	}
	// 同值同摘要（唯一约束同值必同 key 的前提）
	if EscapeValue("has:colon") != a {
		t.Fatal("同值必须同摘要")
	}
}

// 桶 key 生成→反解往返（契约钉死：manager 复核经 ParseEqBucketKey 还原候选——
// 尾缀解析边界（'}' 含入后缀段）曾静默拒解析导致分裂复核全灭）。
func TestBucketKeyParseRoundTrip(t *testing.T) {
	for _, sub := range []int{0, 3, 17} {
		eq := EqBucketKey("users", "idx_city", "hot", 321, sub)
		table, idx, encVal, slot, n, ok := ParseEqBucketKey(eq)
		if !ok || table != "users" || idx != "idx_city" || encVal != "hot" || slot != 321 || n != sub {
			t.Fatalf("eq 往返错位: %q → %v %v %v %v %v %v", eq, table, idx, encVal, slot, n, ok)
		}
		rg := RangeBucketKey("users", "idx_age", 321, sub)
		table, idx, slot, n, ok = ParseRangeBucketKey(rg)
		if !ok || table != "users" || idx != "idx_age" || slot != 321 || n != sub {
			t.Fatalf("range 往返错位: %q → %v %v %v %v %v", rg, table, idx, slot, n, ok)
		}
	}
	// 畸形尾缀显式拒绝（与其余 malformed 分支同纪律）
	if _, _, _, _, _, ok := ParseEqBucketKey("i:users:idx_city=hot:" + SlotTag(321) + "#x7"); ok {
		t.Fatal("非法尾缀必须 ok=false")
	}
	if _, _, _, _, ok := ParseRangeBucketKey("i:users:idx_age:" + SlotTag(321) + "#r-1"); ok {
		t.Fatal("负数子桶必须 ok=false")
	}
	// 含结构字符的值经转义后仍可往返
	v := "has:colon{and}brace"
	eq := EqBucketKey("users", "idx_city", v, 7, 2)
	_, _, encVal, _, n, ok := ParseEqBucketKey(eq)
	if !ok || encVal != EscapeValue(v) || n != 2 {
		t.Fatalf("转义值往返错位: %q → %q n=%d ok=%v", eq, encVal, n, ok)
	}
}
