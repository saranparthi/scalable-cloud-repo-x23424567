# performance_benchmark.py
import time
import psutil
import boto3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

def benchmark_speed_layer():
    """Benchmark speed layer performance"""
    metrics = {
        'latency': [],
        'throughput': [],
        'cpu_usage': [],
        'memory_usage': []
    }
    
    # Test with different workloads
    workloads = [1000, 5000, 10000, 25000, 50000]
    
    for workload in workloads:
        print(f"\nTesting Speed Layer with {workload} records...")
        
        # Start monitoring
        start_time = time.time()
        cpu_before = psutil.cpu_percent(interval=0.5)
        mem_before = psutil.virtual_memory().percent
        
        # Process workload (simulate)
        time.sleep(workload / 1000)  # Simulate processing
        
        # End monitoring
        elapsed_time = time.time() - start_time
        cpu_after = psutil.cpu_percent(interval=0.5)
        mem_after = psutil.virtual_memory().percent
        
        metrics['latency'].append(elapsed_time)
        metrics['throughput'].append(workload / elapsed_time if elapsed_time > 0 else 0)
        metrics['cpu_usage'].append((cpu_before + cpu_after) / 2)
        metrics['memory_usage'].append((mem_before + mem_after) / 2)
    
    return metrics

def benchmark_batch_layer():
    """Benchmark batch layer performance"""
    metrics = {
        'processing_time': [],
        'throughput': [],
        'cpu_usage': [],
        'memory_usage': []
    }
    
    workloads = [10000, 50000, 100000, 200000, 500000]
    
    for workload in workloads:
        print(f"\nTesting Batch Layer with {workload} records...")
        
        # Simulate batch processing
        start_time = time.time()
        cpu_before = psutil.cpu_percent(interval=0.5)
        mem_before = psutil.virtual_memory().percent
        
        # Simulate Spark job
        time.sleep(workload / 5000)
        
        elapsed_time = time.time() - start_time
        cpu_after = psutil.cpu_percent(interval=0.5)
        mem_after = psutil.virtual_memory().percent
        
        metrics['processing_time'].append(elapsed_time)
        metrics['throughput'].append(workload / elapsed_time if elapsed_time > 0 else 0)
        metrics['cpu_usage'].append((cpu_before + cpu_after) / 2)
        metrics['memory_usage'].append((mem_before + mem_after) / 2)
    
    return metrics

def plot_benchmarks(speed_metrics, batch_metrics):
    """Generate benchmark plots"""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Speed Layer - Latency vs Throughput
    axes[0, 0].plot(speed_metrics['throughput'], speed_metrics['latency'], 'bo-')
    axes[0, 0].set_title('Speed Layer: Latency vs Throughput')
    axes[0, 0].set_xlabel('Throughput (records/sec)')
    axes[0, 0].set_ylabel('Latency (seconds)')
    axes[0, 0].grid(True)
    
    # Batch Layer - Processing Time
    workloads = [10000, 50000, 100000, 200000, 500000]
    axes[0, 1].plot(workloads, batch_metrics['processing_time'], 'ro-')
    axes[0, 1].set_title('Batch Layer: Processing Time')
    axes[0, 1].set_xlabel('Records')
    axes[0, 1].set_ylabel('Time (seconds)')
    axes[0, 1].grid(True)
    
    # Resource Usage - Speed
    axes[1, 0].plot(speed_metrics['throughput'], speed_metrics['cpu_usage'], 'go-', label='CPU')
    axes[1, 0].plot(speed_metrics['throughput'], speed_metrics['memory_usage'], 'mo-', label='Memory')
    axes[1, 0].set_title('Speed Layer: Resource Usage')
    axes[1, 0].set_xlabel('Throughput (records/sec)')
    axes[1, 0].set_ylabel('Usage (%)')
    axes[1, 0].legend()
    axes[1, 0].grid(True)
    
    # Resource Usage - Batch
    axes[1, 1].plot(workloads, batch_metrics['cpu_usage'], 'co-', label='CPU')
    axes[1, 1].plot(workloads, batch_metrics['memory_usage'], 'ko-', label='Memory')
    axes[1, 1].set_title('Batch Layer: Resource Usage')
    axes[1, 1].set_xlabel('Records')
    axes[1, 1].set_ylabel('Usage (%)')
    axes[1, 1].legend()
    axes[1, 1].grid(True)
    
    plt.tight_layout()
    plt.savefig('/home/ec2-user/environment/code/benchmarks.png')
    print("Benchmark plots saved to benchmarks.png")

if __name__ == '__main__':
    print("Running Performance Benchmarks...")
    speed_metrics = benchmark_speed_layer()
    batch_metrics = benchmark_batch_layer()
    plot_benchmarks(speed_metrics, batch_metrics)