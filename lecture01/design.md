## Map Reduce Task

###
Structure 
- master.go
    -- split work to all worker.mapper
    -- split work to all worker.reducer
    -- output reducer work
- worker.go
    -- mapper -> read from a file and output to another file (countFile) by hashing
    -- reducer -> read from countFile and save in memory

helpers 
- rpc.go


### Communication between threads (go routines) - CSP
no lock design

### a quick view of the flow
input files
input01.txt, input02.txt, input03.txt


```
input01.txt ->
"a b a c e"
input02.txt ->
"a b e f g"
input03.txt ->
"e e f g"

expected output ->
a 3, b 2, c 1, e 4, f 2, g 2

1. create mappers as the same # of mappers (#input)
mapper1 will handle input01.txt
(1) get all pairs -
<a, 1>
<b, 1>
<a, 1>
<c, 1>
<e, 1>
(2) merge as output
<a, 2>
<b, 1>
<c, 1>
<e, 1>
(3) hash to intermediate file (for example ascii % 2)
1.txt 
<a, 2>
<c, 1>

2.txt
<b, 1>
<e, 1>

2. (main thread) wait group -> make sure all mappers finished

3. create reducers as the same # of intermediate files (#intermediate)
(1) load results from intermediate files and then sort key then overwrite

4. (main thread) wait group -> make sure all reducers finished

5. (main thread) merge all intermediate job  
```

### Bottleneck
1. if single mapper cannot finish a file in a faster way
2. we wait for all mappers to finish then execute reducer
3. 
4. main-thread will sort the result based on the hash intermediate name  
    (requires a good hash function)
    




