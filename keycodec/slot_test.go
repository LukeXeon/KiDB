package keycodec

import "testing"

// XMODEM 标准校验向量（docs/12 §12.2：与 Redis Cluster 规范样例比对）。
func TestCRC16CheckVector(t *testing.T) {
	if got := CRC16("123456789"); got != 0x31C3 {
		t.Fatalf("CRC16(123456789) = %#x, want 0x31C3 (CRC16-XMODEM check)", got)
	}
}

func TestHashtag(t *testing.T) {
	cases := []struct{ key, tag string }{
		{"foo{bar}zap", "bar"},
		{"{bar}", "bar"},
		{"foo{}{bar}", "foo{}{bar}"}, // "{}" 紧邻为空 → 整 key 散列（Redis 规范）
		{"no-tag", "no-tag"},
		{"foo{bar", "foo{bar"}, // 无配对 "}"
		{"d:users:{12345}", "12345"},
	}
	for _, c := range cases {
		if got := hashtag(c.key); got != c.tag {
			t.Errorf("hashtag(%q) = %q, want %q", c.key, got, c.tag)
		}
	}
}

// 同 tag 必同 slot（Redis Cluster 规范示例）。
func TestSlotByTag(t *testing.T) {
	if Slot("foo{bar}zap") != Slot("z{bar}q") {
		t.Fatal("keys sharing tag must share slot")
	}
}

// 16384 个 stag 全部自洽（docs/12 §12.2）。
func TestSlotTagSelfConsistency(t *testing.T) {
	for i := 0; i < NumSlots; i++ {
		tag := SlotTag(uint16(i))
		if got := Slot(tag); got != uint16(i) {
			t.Fatalf("Slot(SlotTag(%d)) = %d", i, got)
		}
	}
}
