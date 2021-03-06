% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ACGI_functions.R
\name{date_vector_exceeds_threshold}
\alias{date_vector_exceeds_threshold}
\title{Identify Date that Value of Vectors Exceeds a Threshold}
\usage{
date_vector_exceeds_threshold(df_dates, df_values, threshold)
}
\arguments{
\item{df_dates}{The date vector from a data frame}

\item{df_values}{The values vector from a data frame which to check against the threshold}

\item{threshold}{Threshold value to scan for in `df_values`}
}
\value{
Date that `threshold` is passed, or `NA` if it is not reached
}
\description{
For a given data frame and corresponding vector of cumulatively summed GDD values (indexed to the same dates),
calculate the date on which a threshold value is passed
}
\examples{
df <- data.frame(
  date = seq(as.Date("1961-07-01"), as.Date("1961-07-31"), "days"),
  value = c(1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9)
  )
date_vector_exceeds_threshold(df$date, df$value, 5)
}
