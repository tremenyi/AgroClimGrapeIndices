% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ACGI_functions.R
\name{tlog}
\alias{tlog}
\title{Log a message with a timestamp}
\usage{
tlog(msg)
}
\arguments{
\item{msg}{Message to log}
}
\value{
\code{cat} a message to the screen with a timestamp with format %X
}
\description{
Log a message with a timestamp
}
\examples{
tlog("Start processing something...")
# ...
tlog("...done")
}
