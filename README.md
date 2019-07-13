# Phenix-Challenge
Challenge provided by Carrefour-Group : https://github.com/Carrefour-Group/phenix-challenge

Implemented features: 1 , 7


## Building
`sbt assembly`

## Testing
`sbt test`

## Running
Run `PhenixApp` onto your IDE

or

`java -Xmx512m -jar phenix-solution-assembly-0.12.0.jar --input_folder INPUT_FOLDER`

or
`sbt run "--input_folder INPUT_FOLDER"`


### Todo

Large files are splitted to be processed, but this approach will have a limit with scala stream type. (Data loaded into memory)
Iterator cannot be used in this project state (Need to refactor)
