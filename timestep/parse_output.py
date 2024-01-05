print("n, t, workers, frac, accesses, time")
with open("args.txt", 'w') as file:
    for frac in [0, 0.0125, 0.025, 0.05, 0.1]:
        for n in [100, 500, 1000]:
            for t in [500, 1000, 2000, 4000, 8000, 16000, 32000, 64000]:
                for acc in [1, 5, 10]:
                    for workers in [1, 2, 4, 8, 15]:
                        try:
                            args = f"-workers {workers} -sleep 1 -strong 0 -steps {n} -isync 0 -restrict 0 -t {t} -deps 1 -frac {frac} -accesses {acc}\n"
                            file_name = f"output/_workers_{workers}__sleep_1__strong_0__steps_{n}__isync_0__restrict_0__t_{t}__deps_1__frac_{frac}__accesses_{acc}.txt"
                            f = open(file_name, "r")
                            lines = f.readlines()
                            time = lines[-1].strip().split(",")[-1].strip()
                            print(f"{n}, {t}, {workers}, {frac}, {acc}, {time}")
                        except:
                            continue
