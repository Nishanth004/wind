import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os

def animate(i):
    if not os.path.exists("log.json"):
        return

    with open("log.json") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return

    legal = sum(1 for x in data if x["status"] == "legal")
    violation = sum(1 for x in data if x["status"] == "violation")
    total = legal + violation

    if total == 0:
        return

    # Clear figure
    plt.clf()

    # 1. Pie chart
    plt.subplot(1, 3, 1)
    plt.pie([legal, violation], labels=["Legal", "Violation"], autopct='%1.1f%%', colors=["green", "red"])
    plt.title("Legal vs Violation")

    # 2. Bar chart
    plt.subplot(1, 3, 2)
    plt.bar(["Legal", "Violation"], [legal, violation], color=["green", "red"])
    plt.title("Bar Chart of Transfers")

    # 3. Time series
    plt.subplot(1, 3, 3)
    times = [x["timestamp"] for x in data]
    values = [1 if x["status"] == "violation" else 0 for x in data]
    plt.plot(times, values, 'ro-', label='Violation')
    plt.title("Time Series (Violations)")
    plt.xticks(rotation=45)

if __name__ == "__main__":
    fig = plt.figure(figsize=(15, 5))
    ani = FuncAnimation(fig, animate, interval=1000, cache_frame_data=False)
    plt.tight_layout()
    plt.show()
