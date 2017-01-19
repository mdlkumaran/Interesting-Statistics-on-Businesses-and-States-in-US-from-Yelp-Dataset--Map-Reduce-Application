OUTPUT / TASKS
--------------

Task 1:  Timely performance of each businesses on different categories (based on user reviews)

Task 2: Category wise performance on each states

Task 3: Top three states that performs well in each categories


Sample DataSet Links  - From S3 Bucket
--------------------

1. business.json
https://s3.amazonaws.com/cloudfinalprojectsubmission/business.json

2. review.json
https://s3.amazonaws.com/cloudfinalprojectsubmission/review.json

3. user.json
https://s3.amazonaws.com/cloudfinalprojectsubmission/user.json


JAR File Links  - From S3 Bucket
-------------- 

1.Task1:
https://s3.amazonaws.com/cloudfinalprojectsubmission/TimelyPerformance_Task1.jar

2.Task2:
https://s3.amazonaws.com/cloudfinalprojectsubmission/StateWise_Task2.jar

3.Task3:
https://s3.amazonaws.com/cloudfinalprojectsubmission/TopThreeStates_Task3.jar


Input Directories
-----------------

/user
/review
/business


Output Directories
------------------

/userid
/reviewOutput
/TimelyPerformance
/output
/t2output1
/t2output2
/job1Output
/topThreeStates


R program:
----------


Commands / Steps for Execution
------------------------------

Step 1: To Create Input Direcories

		hadoop fs -mkdir /business /review /user


Step 2: To Download sample dataset 

		wget https://s3.amazonaws.com/cloudfinalprojectsubmission/business.json

		wget https://s3.amazonaws.com/cloudfinalprojectsubmission/review.json

		wget https://s3.amazonaws.com/cloudfinalprojectsubmission/user.json



Step 3: To copy the sample data set into input directories

		hadoop fs -put business.json /business/

		hadoop fs -put review.json /review/

		hadoop fs -put user.json /user/



Step 4: To Download JAR files:

		wget https://s3.amazonaws.com/cloudfinalprojectsubmission/TimelyPerformance_Task1.jar

		wget https://s3.amazonaws.com/cloudfinalprojectsubmission/StateWise_Task2.jar

		wget https://s3.amazonaws.com/cloudfinalprojectsubmission/TopThreeStates_Task3.jar


Step 5: To execute Tasks or JAR files  and check the Output

		
		1) Task 1

			a) Execute: 

				hadoop jar TimelyPerformance_Task1.jar 

			b) Output: The output directory has all the output files for Task 1. Each file inside this directory is the output for corresponding categories. 


				hadoop fs -ls /output/

				hadoop fs -cat /output/Watches-r-00000 > Watches.csv

				cat Watches.csv 

		1) Task 2

			a) Execute: 

				hadoop jar StateWise_Task2.jar


			b) Output: 

				hadoop fs -cat /t2output2/part-r-00000 > StateWise.csv 
			   
			   	cat StateWise.csv

		1) Task 3

			a) Execute: 

				hadoop jar TopThreeStates_Task3.jar


			b) Output: 

				hadoop fs -cat /topThreeStates/part-r-00000 > TopThreeStates.csv 

				cat TopThreeStates.csv



Output Files
------------

1. Task 1 	- > Watches.csv 

2. Task 2 	- > StateWise.csv

3. Task 3   - > TopThreeStates.csv


Visualization
-------------

Step 1: Open R Studio 

* Note For all steps: please provide absoulte file path of the output file inside the <FilePath> placeholder

Step 2: Execute the below commands in R Studio 

1. Task 1


library(reshape2)

NewData <- read.csv("<FilePath>")

dd1 <- melt(NewData)
dd1$rowid <- 1:5
dd1$rowVal <- dd1$X
ggplot(dd1, aes(variable, value, group=factor(rowVal))) + geom_line(aes(color=factor(rowVal)), size=1) 
legend('bottomright', inset=.05, legend=NewData$X, pch=1, horiz=TRUE, col=1:5)

2. Task 2


library(reshape2)

NewData <- read.csv("<FilePath>")

for (var in unique(NewData$State)) {
  dev.new()
  nd <- NewData[NewData$State==var,]
  print(ggplot(nd, aes(x=nd$Category, y=nd$Value)) + geom_bar(stat = "identity"))
}


3. Task 3

library(reshape2)
library(ggplot2)
dataSet <- data.frame(read.csv("<FilePath>"))
par(mfrow=c(1,3))
rank1 <- barplot(dataSet$State_Value1, main="1st", horiz = TRUE, names.arg = dataSet$Categories, cex.names = 0.8, las=2, col = "green")
text(0, rank1, dataSet$State_Rank1, cex=1, pos=4, col = "white")
rank2 <- barplot(dataSet$State_Value2, main="2nd", horiz = TRUE, cex.names = 0.8, las=2, col = "blue")
text(0, rank2, dataSet$State_Rank2, cex=1, pos=4, col = "white")
rank3 <- barplot(dataSet$State_Value3, main="3rd", horiz = TRUE, las=2, col = "red")
text(0, rank3, dataSet$State_Rank3, cex=1, pos=4, col = "white")


		




