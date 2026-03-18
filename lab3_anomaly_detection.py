import time
import random
import csv
import os
import statistics
from collections import deque

# ==========================================
# Data Engineering - Lab 3: Anomaly Detection & Edge Robustness
# ==========================================

RAW_DATA_FILE = "raw_noisy_data.csv"
CLEAN_DATA_FILE = "clean_filtered_data.csv"
TOTAL_SAMPLES = 500
WINDOW_SIZE = 10  
MODIFIED_Z_THRESHOLD = 3.5 # Matches the lecture slides

def unstable_sensor_stream(num_samples):
    """Simulates a sensor with normal readings and occasional massive impulse noise."""
    for i in range(num_samples):
        if random.random() < 0.10:
            yield random.choice([0.0, 100.0]) # Flash crash / Spike
        else:
            yield 25.0 + random.uniform(-1.0, 1.0) # Normal reading

def process_without_filter():
    print(f"\n[*] Processing {TOTAL_SAMPLES} samples WITHOUT filtering...")
    sensor = unstable_sensor_stream(TOTAL_SAMPLES)
    anomalies_recorded = 0
    with open(RAW_DATA_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        for value in sensor:
            writer.writerow([value])
            if value > 50.0 or value < 10.0:
                anomalies_recorded += 1
    print(f"    -> Warning: {anomalies_recorded} anomalies written to database!")

def process_with_mad_filter():
    print(f"\n[*] Processing {TOTAL_SAMPLES} samples WITH MAD Filter...")
    sensor = unstable_sensor_stream(TOTAL_SAMPLES)
    
    window = deque(maxlen=WINDOW_SIZE)
    anomalies_rejected = 0
    
    with open(CLEAN_DATA_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        for value in sensor:
            # Warm-up phase
            if len(window) < WINDOW_SIZE:
                window.append(value)
                writer.writerow([value])
                continue
                
            # TODO 1: Calculate the 'current_median' of the values in the 'window'
            current_median = statistics.median(window)
            
            # TODO 2: Calculate the Absolute Deviations for each item in the window
            absolute_deviations = [abs(x - current_median) for x in window]
            
            # TODO 3: Calculate the Median Absolute Deviation (MAD)
            # Hint: Add 0.0001 to avoid ZeroDivisionError.
            mad = statistics.median(absolute_deviations) + 0.0001
            
            # TODO 4: Calculate the Modified Z-Score (M)
            # Formula: M = 0.6745 * (value - current_median) / mad
            M = 0.6745 * (value - current_median) / mad

            
            # TODO 5: Reject or Accept the value based on MODIFIED_Z_THRESHOLD
            # If accepted, remember to append it to the window and write to the CSV.
            # If rejected, increment anomalies_rejected.
            if abs(M) <= MODIFIED_Z_THRESHOLD:
                window.append(value)
                writer.writerow([value])
            else:
                anomalies_rejected += 1
            #pass  Remove this pass when you implement the logic
            
    print(f"    -> Success: {anomalies_rejected} anomalies successfully blocked!")

if __name__ == "__main__":
    print("=== Edge Pipeline: Robustness & Anomaly Detection ===")
    process_without_filter()
    process_with_mad_filter()
