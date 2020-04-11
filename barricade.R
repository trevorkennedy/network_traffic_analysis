# Clear workspace and console
rm(list = ls())
cat("\014")

# Load libraries
library(jsonlite)
library(plyr)
library(dplyr)

# Read data
json_file <- "https://raw.githubusercontent.com/trevorkennedy/public/master/barricade/data.json"
json_data <- fromJSON(json_file, flatten=TRUE)

# Rename features
colnames(json_data) <- c("index", "type", "id", "score", "reqtype", "rawlen", "dport", "created", "sport", "hostname", "requestmeta", "dst", "src", "license", "proto", "message", "pcre", "importance", "reference")

# Print frequencies
count(json_data, "index")
count(json_data, "type")
count(json_data, "id")
count(json_data, "score")
count(json_data, "reqtype")
count(json_data, "rawlen")
count(json_data, "dport")
count(json_data, "created")
count(json_data, "sport")
count(json_data, "hostname")
count(json_data, "requestmeta")
count(json_data, "sport")
count(json_data, "dst")
count(json_data, "src")
count(json_data, "license")
count(json_data, "proto")
count(json_data, "message")
count(json_data, "pcre")
count(json_data, "importance")
count(json_data, "reference")

# Drop features that are either constant, redundant or too sparse
json_data$index <- NULL
json_data$type <- NULL
json_data$id <- NULL
json_data$score <- NULL
json_data$reqtype <- NULL
json_data$hostname <- NULL
json_data$requestmeta <- NULL
json_data$license <- NULL
json_data$message <- NULL
json_data$pcre <- NULL
json_data$reference <- NULL

# Select labeled cases (importance field)
flagged <- subset(json_data, json_data$importance %in% c("event","attack"), select=c(created))
flagged$flagged <- 1

# Create new features
json_data$tcp <- ifelse(json_data$proto == "tcp", 1, 0)
json_data$arp <- ifelse(json_data$proto == "arp", 1, 0)
json_data$udp <- ifelse(json_data$proto == "udp", 1, 0)
json_data$host <- paste(json_data$src, json_data$dst, sep = " > ")
json_data$port <- ifelse(json_data$proto == "arp", "", paste(json_data$sport, json_data$dport, sep = " > "))

# Drop features no longer needed
json_data$src <- NULL
json_data$dst <- NULL
json_data$sport <- NULL
json_data$dport <- NULL
json_data$proto <- NULL
json_data$importance <- NULL

# Remove label rows
json_data <- subset(json_data, !(is.na(json_data$rawlen)))

# Left join labels using time stamp (not ideal because of duplicates)
df <- left_join(json_data, flagged)

# Create aggregated features
summary <- summarise(group_by(df, host),
          unique_ports = n_distinct(port),
          connections = n(),
          # Calculate connections per minute
          connect_rate = as.numeric(strptime(last(created), "%FT%T") - strptime(first(created), "%FT%T")) / n(),
          percent_tcp = sum(tcp) / n(),
          percent_udp = sum(udp) / n(),
          percent_arp = sum(arp) / n(),
          max_length = max(rawlen),
          # Create binary dependant variable
          flagged = ifelse( sum(flagged, na.rm = TRUE) >= 1, TRUE, FALSE)
)

# Clean up
rm(flagged, json_data, json_file, df)

# Write to file
write.csv(summary, "summary.csv", row.names=FALSE)