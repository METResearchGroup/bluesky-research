"""Experiments with adding Bloom filters to the sync stream."""
import time

test_set = set()
for i in range(3000000):
    test_set.add(f"{i=}")

print(len(test_set))

start_time = time.time()

# Test 1: an element that is in the set
print("Test 1: an element that is in the set")
i = 1000000
set_elem = f"{i=}"
if set_elem in test_set:
    print("Element is in the set")
else:
    print("Element is not in the set")

# Test 2: an element that is not in the set
print("Test 2: an element that is not in the set")
elem_not_in_set = "2000000"
if elem_not_in_set in test_set:
    print("Element is in the set")
else:
    print("Element is not in the set")

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
