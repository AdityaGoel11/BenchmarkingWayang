def increase_file_size(input_file, output_file, target_size_mb):
    with open(input_file, 'r') as original:
        content = original.read()

    with open(output_file, 'w') as output:
        target_size_bytes = target_size_mb * 1024 * 1024
        while output.tell() < target_size_bytes:
            output.write(content)

input_file_path = 'harry.txt'  # Replace with the path to your original text file
output_file_path = 'large_file.txt'  # Replace with the desired output file path
target_size = 1000  # Target size in MB

increase_file_size(input_file_path, output_file_path, target_size)
