### Preparation phase 
- [X] setup of the local dev environement
- [X] create a copy of the notebook
- [X] ensure that the current notebook run without errors
- [X] identify code smells : 
          [X] dead code (executed code with a result never reused) => print, display, show, df.printSchema ...
          [X] exposing explicity implementation details
          [X] duplication
          [X] magic command (%sql, %scala, %python)
          [X] to many comments ..etc
- [X] convert the notebook into a python file
- [X] remove dead codes
- [X] group codes if possible into 3 sections (global functions): extract(), transoform(), load() 
- [x] resolve dependencies between sections (extract(), transoform(), load()) and ensure that the notebook run successufily 
- [X] Start the refactoring phase of the 3 sections (one by one) 

### Refactoring phase
- [X] run a characterisation test (pytest-watch)
- [X] do()
    - [X] identify a block of code that can be exported to a python module
    - [X] write the test for the python module
    - [X] write the python module 
    - [X] make the test pass (read and analyze continuous feedback from pytest-watch)
    - [X] use the python function in the main code
    - [X] commit the last changes
    - [X] refactor again ...
