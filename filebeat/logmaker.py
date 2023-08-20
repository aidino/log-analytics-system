import time

def main():
    input_file_path = "../data/raw/BGL_test2.log"
    output_file_path = "log/output.log"
    
    print("Start writing to output file...")
    with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
        for line in input_file:
            output_file.write(line)
            output_file.flush()  # Flush the buffer to ensure immediate writing
            time.sleep(0.1)  # Wait for 1 second
        print("Done")

if __name__ == "__main__":
    main()
