---
title: LLM Simulator
description: Introduction to the LLM Simulator.
---

The LLM Simulator is a comprehensive performance modeling and simulation tool designed for analyzing Large Language Model (LLM) inference workloads. This tool provides detailed insights into computational performance, memory usage, and system behavior under various hardware configurations and request patterns.

The simulator consists of several key components that work together to model the complete lifecycle of LLM inference requests:

- **Request Modeling**: Simulates incoming generation requests with configurable arrival patterns
- **Engine Simulation**: Models LLM inference engines with prefill and decode phases
- **Performance Analysis**: Provides roofline analysis and hardware-specific performance metrics
- **Memory Management**: Tracks memory allocation and deallocation across multiple concurrent requests
- **Trace Generation**: Outputs detailed execution traces for visualization and analysis

## Install

```bash
git clone git@github.com:eth-easl/Scratchpad.git
cd tools/simulator
```

## Architecture

### Core Components

#### Request Module (`core/request.py`)
The `GenerationRequest` class represents individual inference requests with the following attributes:
- `req_id`: Unique identifier for the request
- `model`: Target model name (e.g., "meta-llama/Llama-2-7b-hf")
- `input_length`: Number of input tokens
- `output_length`: Number of output tokens to generate
- `arrive_at`: Timestamp when the request arrives
- `status`: Current state (PENDING, SCHEDULED, PREFILL, GENERATE, EXIT)

Requests progress through different states:
1. **PENDING**: Waiting to be processed
2. **PREFILL**: Initial input processing phase
3. **GENERATE**: Sequential token generation phase
4. **EXIT**: Request completed

#### Engine Module (`core/engine.py`)
The `LLMEngine` class simulates the actual inference execution:

**Key Features:**
- Manages request queues (waiting, running, finished, failed)
- Handles memory allocation through a memory planner
- Executes prefill and decode phases with accurate timing
- Generates trace events for performance analysis

**Execution Phases:**
- **Prefill Phase**: Processes all input tokens at once, computes key-value caches
- **Decode Phase**: Generates output tokens one at a time, using cached keys and values

#### Trace Module (`core/trace.py`)
The `TraceEvent` dataclass captures execution events for performance analysis:
- Events include prefill, decode, and memory operations
- Timestamps are recorded in microseconds for Chrome Trace Format compatibility
- Supports both duration events (`ph="X"`) and counter events (`ph="C"`)

#### Hardware Module (`config/hardware_params.py`)
Contains detailed specifications for various GPU hardware platforms:

**Supported Hardware:**
- NVIDIA A100 (40GB/80GB variants)
- NVIDIA H100 (SXM/PCIe variants)
- NVIDIA A40
- NVIDIA L40

**Parameters per Hardware:**
- Memory bandwidth (bytes/second)
- Peak compute performance (FLOPS) for different precision levels
- On-chip buffer size

## Usage

### Running a Complete Simulation

The main simulation entry point is `cli/start_simulator.py`:

```bash
python cli/start_simulator.py \
  --input input/trace_file.json \
  --n-engines 4 \
  --arrival-rate 2.0 \
  --trace-output output/trace.json \
  --stats-output output/stats.json
```

**Parameters:**
- `--input`: JSON file containing request traces (each line with "input" and "output" token counts)
- `--n-engines`: Number of LLM engines to simulate.
- `--arrival-rate`: Request arrival rate (requests per second).
- `--trace-output`: Output file for Chrome trace format events
- `--stats-output`: Output file for simulation statistics

### Input Trace Format

The input trace file should contain one JSON object per line:
```json
{"id": "a-1234", "status": "DEFAULT", "created_at": "2024-07-17 13:56:49.399", "finished_at": "2024-07-17 13:56:50.527", "model": "meta-llama/Meta-Llama-3-8B-Instruct", "model_parameters": {"top_p": 1, "max_tokens": 256, "temperature": 0, "presence_penalty": 0, "frequency_penalty": 0}, "reported_token_input": 23, "reported_token_output": 38}
```

### Output Files

1. **Trace Output** (`trace.json`): Chrome Trace Format file containing:
   - Prefill and decode events with timing information
   - Memory usage counters
   - Request lifecycle events

2. **Stats Output** (`stats.json`): Summary statistics including:
   - Request completion times
   - System utilization metrics
   - Failed request information
   - Configuration details

### Reading the output trace

The generated trace can be viewed in Chrome's trace viewer:
- Open Chrome and navigate to `chrome://tracing`.
- Load the `trace.json` file to visualize request timelines, engine utilization, and memory usage.

## Utility Functions

### Performance Calculations (`utils.py`)

Key functions for performance modeling:

- `flops_matmul(b, m, n, k, rank=None)`: Calculate FLOPS for matrix multiplication
- `memory_matmul(b, m, n, k, w_bit, a_bit, rank=None)`: Calculate memory access patterns
- `roofline_analyze(bandwidth, max_OPS, OPs, memory_access)`: Roofline performance analysis
- `get_linear_layers(...)`: Extract linear layer dimensions from model configuration

### Model Layer Analysis

The simulator automatically extracts linear layer dimensions from transformer models:
- Query, Key, Value projections
- Output projection
- Feed-forward gate, up, and down projections
- Supports tensor parallelism with TP size > 1

## Request Processing Flow

1. **Request Arrival**: Requests are added to the waiting queue
2. **Memory Check**: System verifies if sufficient memory is available
3. **Prefill Execution**: Input tokens are processed, KV cache is built
4. **Decode Loop**: Output tokens are generated sequentially
5. **Memory Cleanup**: Memory is freed when requests complete

## Performance Metrics

The simulator tracks multiple performance metrics:

- **Latency**: Total time from request arrival to completion
- **Throughput**: Requests processed per second
- **Memory Utilization**: Peak and average memory usage
- **Hardware Utilization**: Percentage of peak theoretical performance
- **Queue Times**: Time spent waiting vs processing

## Advanced Features

### Memory Planning
The system includes sophisticated memory management:
- Tracks memory blocks for KV cache storage
- Handles allocation failures gracefully
- Supports different precision levels (weights, activations, KV cache)

### Batch Processing
During decode phase, multiple requests can be processed together:
- Dynamic batching based on memory availability
- Batch size affects compute efficiency
- Supports heterogeneous batch sizes

### Trace Analysis
Generated traces can be loaded into Chrome's trace viewer (chrome://tracing) for detailed visual analysis of:
- Request timelines
- Engine utilization
- Memory usage patterns
- Concurrent execution

## Example Use Cases

1. **Hardware Selection**: Compare different GPU configurations for specific workloads
2. **Sizing Studies**: Determine optimal engine count for target performance
3. **Bottleneck Analysis**: Identify whether system is memory or compute bound
4. **Capacity Planning**: Estimate required resources for expected request patterns
5. **Algorithm Design**: Evaluate impact of different batching strategies

## Dependencies

The simulator requires several external libraries:
- `transformers`: For model configuration loading
- `humanize`: For formatting large numbers
- `numpy`: For numerical computations
- `matplotlib`/`seaborn`: For plotting roofline graphs
- `rich`: For enhanced console output

## Configuration

Hardware parameters can be extended by modifying `config/hardware_params.py`. Each hardware platform should specify:
- Memory bandwidth in bytes/second
- Peak compute performance for relevant precision levels
- On-chip buffer size in bytes

## Limitations and Assumptions

- Models inference timing based on analytical models rather than actual execution
- Assumes static hardware parameters (no thermal throttling or frequency scaling)
- Simplified memory model (doesn't account for fragmentation overhead)
- Network latency and I/O overhead are not modeled
- Request scheduling follows FIFO order with simple memory-based admission control