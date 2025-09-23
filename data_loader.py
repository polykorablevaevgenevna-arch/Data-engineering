#скачивание файла из MiniO с помощью Anaconda prompt
curl -k -o GSE13159_RAW.tar "https://94.124.179.195:9000/poly-bio-data/GSE13159_RAW.tar"

#разархивирование файла с помощью программы BreeZip

#В языке R преобразование файлов из формата .CELL в формат .csv с помощью скрипта (он также показывает первые 6 записей в таблице:
library(affy)
cel_dir <- "C:\\data_eng\\GSE13159_RAW\\GSM329727.CEL"
data <- ReadAffy(celfile.path = cel_dir)
eset <- rma(data)
exprs_data <- exprs(eset)
head(exprs_data)

#В Python с помозью этого скрипта выводила первые 10 строчек
raw_data = pd.read_csv("C:\data_eng\expression_data.csv")
raw_data.head(10) 
