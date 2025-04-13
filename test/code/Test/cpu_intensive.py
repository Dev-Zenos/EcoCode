def prime_finder(limit):
    """Find all prime numbers up to the specified limit using trial division."""
    primes = []
    for num in range(2, limit + 1):
        is_prime = True
        # Trial division for each number
        for i in range(2, int(num ** 0.5) + 1):
            if num % i == 0:
                is_prime = False
                break
        if is_prime:
            primes.append(num)
    return primes

def matrix_multiplication(size):
    """Perform an inefficient matrix multiplication to consume CPU."""
    matrix_a = [[i*j for j in range(size)] for i in range(size)]
    matrix_b = [[j*i for j in range(size)] for i in range(size)]
    result = [[0 for _ in range(size)] for _ in range(size)]
    
    for i in range(size):
        for j in range(size):
            for k in range(size):
                result[i][j] += matrix_a[i][k] * matrix_b[k][j]
    return result

def recursive_fibonacci(n):
    """Calculate Fibonacci numbers recursively (very inefficient for large n)."""
    if n <= 1:
        return n
    else:
        return recursive_fibonacci(n-1) + recursive_fibonacci(n-2)

def main():
    print("Starting CPU-intensive tasks...")
    
    # Run multiple CPU-intensive tasks in parallel
    while True:
        # Task 1: Find primes up to a large number
        primes = prime_finder(10000)
        print(f"Found {len(primes)} prime numbers")
        
        # Task 2: Perform large matrix multiplication
        result = matrix_multiplication(200)
        print(f"Matrix multiplication complete, result size: {len(result)}x{len(result[0])}")
        
        # Task 3: Calculate Fibonacci numbers using inefficient recursive approach
        fib_num = recursive_fibonacci(35)
        print(f"Fibonacci calculation complete: {fib_num}")
        
        # Task 4: Consume CPU with floating-point operations
        float_result = 0.0
        for i in range(10000000):
            float_result += (i * 1.1) / (i + 1.1)
        print(f"Floating point operations complete: {float_result:.2f}")

if __name__ == "__main__":
    main()
