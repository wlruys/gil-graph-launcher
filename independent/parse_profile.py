import os

print("n, t, workers, frac, accesses, time")
for frac in [0, 0.0125, 0.025, 0.05, 0.1, 0.2]:
    for n in [500, 1000]:
        for t in [500, 1000, 2000, 4000, 8000, 16000, 32000, 64000]:
            for acc in [1, 5, 10]:
                for workers in [1, 2, 4, 8, 15]:
                    file_name=f"profile/_{n}__t_{t}__workers_{workers}__frac_{frac}__accesses_{acc}.qdrep"
                    os.system(f"nsys stats {file_name}")
