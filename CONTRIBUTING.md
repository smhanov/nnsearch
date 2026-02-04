# Contributing to nnsearch

Thank you for your interest in contributing to nnsearch! This document provides guidelines and information about contributing to this project.

## 🐛 Reporting Bugs

Before reporting a bug, please:

1. Check the [existing issues](https://github.com/smhanov/nnsearch/issues) to see if it's already been reported
2. Make sure you're using the latest version

When reporting a bug, please include:

- Go version (`go version`)
- Operating system and architecture
- A minimal code example that reproduces the issue
- The expected vs actual behaviour
- Any error messages or logs

## 💡 Feature Requests

We welcome feature requests! Please:

1. Check existing issues to avoid duplicates
2. Clearly describe the use case
3. Explain why this feature would be useful to others

## 🔧 Pull Requests

### Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/nnsearch.git
   cd nnsearch
   ```
3. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. Make your changes
5. Run tests:
   ```bash
   go test ./...
   ```
6. Commit with a clear message:
   ```bash
   git commit -m "Add feature: description of your changes"
   ```
7. Push and create a Pull Request

### Code Style

- Follow standard Go conventions and [Effective Go](https://golang.org/doc/effective_go)
- Run `gofmt` before committing
- Add comments for exported functions and types
- Keep functions focused and reasonably sized

### Testing

- Add tests for new functionality
- Ensure existing tests pass
- For performance-related changes, include benchmark results

### Documentation

- Update the README if adding new features
- Add godoc comments for public APIs
- Include examples where appropriate

## 📋 Development Setup

```bash
# Clone the repository
git clone https://github.com/smhanov/nnsearch.git
cd nnsearch

# Download dependencies
go mod download

# Run tests
go test ./...

# Run the demo
go run cmd/demo/main.go -h
```

## 🏗️ Project Structure

```
nnsearch/
├── cmd/demo/         # Demo command-line program
├── metricspace.go    # Core interfaces (MetricSpace, SpaceIndex)
├── graph.go          # Graph-based index implementation
├── pivots.go         # Pivot selection algorithms
├── freezer.go        # Index serialization
├── readwrite.go      # Binary I/O utilities
├── utils.go          # Utility functions (parallel loops, etc.)
├── wordvecs.go       # Word vector support
└── wmdcalc.go        # Word Mover's Distance
```

## 🎯 Areas for Contribution

We're particularly interested in contributions for:

- **New distance functions**: Implementations for common metrics
- **Performance optimizations**: Faster index construction or queries
- **Better pivot selection**: Improved initialization strategies
- **Documentation**: Tutorials, examples, benchmarks
- **Testing**: More comprehensive test coverage

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License.

## 💬 Questions?

Feel free to open an issue for any questions about contributing!
