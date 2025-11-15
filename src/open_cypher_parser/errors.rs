use nom::error::{ContextError, ParseError};
use std::fmt;

#[derive(Debug, PartialEq)]
pub struct OpenCypherParsingError<'a> {
    pub errors: Vec<(&'a str, &'static str)>,
}

impl<'a> ParseError<&'a str> for OpenCypherParsingError<'a> {
    fn from_error_kind(input: &'a str, _kind: nom::error::ErrorKind) -> Self {
        OpenCypherParsingError {
            errors: vec![(input, "unknown error")],
        }
    }

    fn append(input: &'a str, _kind: nom::error::ErrorKind, mut other: Self) -> Self {
        other.errors.push((input, "unknown error (appended)"));
        other
    }
}

impl<'a> ContextError<&'a str> for OpenCypherParsingError<'a> {
    fn add_context(input: &'a str, ctx: &'static str, mut other: Self) -> Self {
        other.errors.push((input, ctx));
        other
    }
}

impl fmt::Display for OpenCypherParsingError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // if *&self.errors.last().is_some() {
        //     let (input, ctx) = &self.errors.last().unwrap();
        //     writeln!(f, "{}: {:}", ctx, input)?;
        // }
        for (input, ctx) in &self.errors {
            writeln!(f, "{}: {:}", ctx, input)?;
        }
        Ok(())
    }
}

impl<'a> From<nom::error::Error<&'a str>> for OpenCypherParsingError<'a> {
    fn from(err: nom::error::Error<&'a str>) -> Self {
        OpenCypherParsingError {
            // errors: vec![(err.input, "nom::error conversion")],
            errors: vec![(err.input, "Unable to parse")],
        }
    }
}
