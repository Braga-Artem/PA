import os
import random
import struct
import tempfile
import heapq

def generate_large_file(file_path, size_in_mb):
    """Створення великого бінарного файлу із випадковими цілими числами."""
    num_integers = size_in_mb * 1024 * 1024 // 4  # Each integer is 4 bytes
    with open(file_path, 'wb') as f:
        for _ in range(num_integers):
            f.write(struct.pack('i', random.randint(0, 1000000)))

def read_integers_from_file(file_path):
    """Читання цілих чисел із двійкового файлу."""
    with open(file_path, 'rb') as f:
        while chunk := f.read(4):
            yield struct.unpack('i', chunk)[0]

def write_integers_to_file(file_path, integers):
    """Записати цілі числа у двійковий файл."""
    with open(file_path, 'wb') as f:
        for num in integers:
            f.write(struct.pack('i', num))

def print_file_content(file_path, limit=100):
    """Друк вмісту двійкового файлу як цілих чисел до вказаної межі."""
    with open(file_path, 'rb') as f:
        count = 0
        while chunk := f.read(4):
            print(struct.unpack('i', chunk)[0], end=' ')
            count += 1
            if count >= limit:
                print("...")
                break

def convert_to_text_file(binary_file, text_file):
    """Перетворення двійкового файлу у текстовий для зручного перегляду."""
    with open(binary_file, 'rb') as bin_f, open(text_file, 'w') as txt_f:
        for number in read_integers_from_file(binary_file):
            txt_f.write(f"{number}\n")

def balanced_multiway_merge_sort(input_file, output_file, temp_dir, memory_limit_mb):
    """Виконаня збалансованого багатостороннього сортування злиттям у великому файлі"""
    chunk_size = memory_limit_mb * 1024 * 1024 // 4  # Number of integers per chunk
    temp_files = []

    #Розділення файлів на відсортовані частини
    with open(input_file, 'rb') as f:
        while chunk := f.read(chunk_size * 4):
            integers = list(struct.unpack(f'{len(chunk) // 4}i', chunk))
            integers.sort()

            temp_file = tempfile.NamedTemporaryFile(delete=False, dir=temp_dir)
            write_integers_to_file(temp_file.name, integers)
            temp_files.append(temp_file.name)

    #Об’єднання відсортованих фрагментів за допомогою черги пріоритетів
    min_heap = []
    file_pointers = []

    for temp_file in temp_files:
        fp = open(temp_file, 'rb')
        file_pointers.append(fp)
        num = struct.unpack('i', fp.read(4))[0]
        heapq.heappush(min_heap, (num, temp_file))

    with open(output_file, 'wb') as out:
        while min_heap:
            smallest, source_file = heapq.heappop(min_heap)
            out.write(struct.pack('i', smallest))

            source_fp = file_pointers[temp_files.index(source_file)]
            next_chunk = source_fp.read(4)
            if next_chunk:
                next_num = struct.unpack('i', next_chunk)[0]
                heapq.heappush(min_heap, (next_num, source_file))

    # Закриття та очистка тимчасових файлів
    for fp in file_pointers:
        fp.close()

    for temp_file in temp_files:
        os.remove(temp_file)


input_file = 'large_random_numbers.bin'
output_file = 'sorted_numbers.bin'
temp_dir = 'temp_sorting'

os.makedirs(temp_dir, exist_ok=True)

# Генерація файлу розміром 10Мб
print("Generating input file...")
generate_large_file(input_file, 10)

# Відсортування файлу із обмеженням пам’яті 100 Мб
print("Sorting file...")
balanced_multiway_merge_sort(input_file, output_file, temp_dir, memory_limit_mb=100)

# перевірка вихідних файлів
print("Verifying sorted output...")
sorted_numbers = list(read_integers_from_file(output_file))
assert sorted_numbers == sorted(sorted_numbers)
print("Sorting successful!")

# Друк вмісту вхідних і вихідних файлів
print("\nContent of input file:")
print_file_content(input_file)
print("\nContent of output file:")
print_file_content(output_file)

# конвертація файлів в текстовий документ для перегляду роботи алгоритму
convert_to_text_file(input_file, 'input_numbers.txt')
convert_to_text_file(output_file, 'sorted_numbers.txt')
print("\nInput and output files have been converted to text files: 'input_numbers.txt', 'sorted_numbers.txt'")