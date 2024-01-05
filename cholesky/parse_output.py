print("n, t, workers, frac, accesses, time")
for frac in [0, 0.0125, 0.025, 0.05, 0.1, 0.2]:
    for b in [10, 20, 40]:
        for t in [500, 1000, 2000, 4000, 8000, 16000, 32000]:
            for acc in [1, 5, 10]:
                for workers in [1, 2, 4, 8, 15]:
                    try:
                        args = f"-b {b} -t {t} -workers {workers} -frac {frac} -accesses {acc}\n"
                        file_name=f"output/_b_{b}__t_{t}__workers_{workers}__frac_{frac}__accesses_{acc}.txt"
                        f = open(file_name, "r")
                        lines = f.readlines()
                        time = lines[-1].strip().split(",")[-1].strip()
                        assert(float(time))
                        print(f"{b}, {t}, {workers}, {frac}, {acc}, {time}")
                    except:
                        pass
