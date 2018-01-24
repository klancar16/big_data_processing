with open('data/primes1.txt') as f:
    lines = f.readlines()
numbers = [int(x) for line in lines for x in line.split() if x.isdigit()]
print(sum(numbers)/len(numbers))
