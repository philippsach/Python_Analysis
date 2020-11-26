from multiprocessing import Pool
import multiprocessing as mp
import numpy as np
import pandas as pd

overview_file = pd.read_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/art_xml_tobescraped_current_status.csv")

def f(x):
    return x*x

if __name__ == "__main__":
    with Pool(5) as p:
        print(p.map(f, [1,2,3]))

print("Number of processors: ", mp.cpu_count())


df = pd.DataFrame(np.random.randint(3, 10, size= [5, 2]))
print(df.head())


def hypotenuse(row):
    return round(row[0]**2 + row[1]**2, 2)**0.5


def easyfunction(row):
    return [row[0] + row[1], row[0] * row[1]]


for row in df.itertuples(index=False, name = None):
    print(row)
    

for row in overview_file.head(10).itertuples(index=False, name = None):
    print(row)

with mp.Pool(4) as pool:
    result = pool.map(easyfunction, df.itertuples(index=False, name=None), chunksize=4)
    

#print(result)

# testing easy function
print("only testing from now on")
print("result of easy function of first row: ", easyfunction(df.iloc[0]))

# testing hypotenuse function
print(hypotenuse(df.iloc[0]))

print(df.itertuples(name=None))

# create as many processes as there are CPUs on this machine
num_processes = mp.cpu_count()
print("df.shape[0]: ", df.shape[0])

# calculate the chunk size as an integer
chunk_size = int(df.shape[0]/num_processes)
print("calculated chunk size: ", chunk_size)

df[2], df[3] = ["dogs", 30]
