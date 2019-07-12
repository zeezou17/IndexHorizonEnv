with open('../temp.sql', 'r') as myfile1:
    all_lines = myfile1.read()
    #print(len(all_lines))
    word_count = 0
    line_count = 0
    for line in all_lines.splitlines():
        word_count += len(line)
        line_count += 1
    #print(word_count)
    #print(line_count)
str_1 = 'co11'
str_2 = 'c1.col2'
#print(str_1.split('.')[-1])
#print(str_2.split('.')[-1])
