package utils

// set.go：泛型集合——map[K]struct{} 惯用法的类型化封装（v7.0 触发三：
// 非泛型/手搓容器语义显式化）。零开销：编译期与裸 map 完全等价，
// range/len/make 直接可用；容量场景用 make(Set[K], n)，带初值用 NewSet。

// Set 是 K 的存在性集合。
type Set[K comparable] map[K]struct{}

// NewSet 以初值构造（容量场景请用 make(Set[K], n)）。
func NewSet[K comparable](items ...K) Set[K] {
	s := make(Set[K], len(items))
	for _, k := range items {
		s.Add(k)
	}
	return s
}

// Add 加入元素（幂等）。
func (s Set[K]) Add(k K) { s[k] = struct{}{} }

// Has 存在性判定。
func (s Set[K]) Has(k K) bool { _, ok := s[k]; return ok }

// Del 移除元素。
func (s Set[K]) Del(k K) { delete(s, k) }
