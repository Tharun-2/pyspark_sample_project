import os
import csv
import random
import time
from datetime import datetime

OUTPUT_DIR = "/home/mt24052/pyspark_file_watcher/input"  
# os.makedirs(OUTPUT_DIR, exist_ok=True)

# Sample data options
PLANT_NAMES = [
    "Artichoke", "Arugula", "Asparagus", "Beans, bush",
    "Beans, lima (bush)", "Beans, pole", "Beet", "Broccoli",
    "Brussels sprouts", "Cabbage", "Carrot", "Cauliflower"
]

UOM_VALUES = ["F", "I"]
ROWS_PER_FILE = 10

# File counter
file_counter = 1

print(f"Starting file generation every 1 minute in: {OUTPUT_DIR}")
while True:
    filename = f"veg_plant_height{file_counter}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["plant_name", "UOM", "Low_End_of_Range", "High_End_of_Range"])

        for _ in range(ROWS_PER_FILE):
            plant = random.choice(PLANT_NAMES)
            uom = random.choice(UOM_VALUES)
            low = random.randint(2, 20)
            high = random.randint(low + 1, low + 10)
            writer.writerow([plant, uom, low, high])

    print(f"[{datetime.now()}] Generated: {filename}")
    file_counter += 1
    time.sleep(60)  # wait 60 seconds before next file
