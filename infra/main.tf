provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw" {
  bucket = "eia-amex_credit_cards-raw"
}

resource "aws_s3_bucket" "processed" {
  bucket = "eia-amex_credit_cards-processed"
}
