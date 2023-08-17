import time

def main():
    input_file_path = "../data/raw/BGL_test1.log"
    output_file_path = "log/output.log"
    

    with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
        for line in input_file:
            output_file.write(line)
            output_file.flush()  # Flush the buffer to ensure immediate writing
            time.sleep(1)  # Wait for 1 second

if __name__ == "__main__":
    main()
