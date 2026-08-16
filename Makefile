# KiDB 开发门禁（约定优于配置：命令即文档，Makefile 只做快捷方式）。
.PHONY: build test vet lint wire

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

# 静态检查（golangci-lint 以 go tool 指令入库，配置见 .golangci.yml）
lint:
	go tool golangci-lint run ./...

# wire 重新生成（纪律：裸 wire 二进制不在 PATH 时 go:generate 静默失败——
# 先删旧产物再经 module graph 运行，实证教训见 CHANGELOG v6.x）
wire:
	cd di && rm -f wire_gen.go && go run -mod=mod github.com/google/wire/cmd/wire
