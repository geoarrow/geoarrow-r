
#pragma once

#include <cstring>
#include <sstream>
#include <cstdlib>

#include "handler.hpp"
#include "io.hpp"

#ifdef FASTFLOAT_FAST_FLOAT_H
#define _GEOARROW_FROM_CHARS(first, last, out) fast_float::from_chars(first, last, out)
#else
namespace {

class from_chars_output_type {
public:
  std::errc ec;
};

from_chars_output_type from_chars_internal(const char* first, const char* last, double& out) {
  from_chars_output_type answer;
  answer.ec = std::errc();

  char* end_ptr;
  out = std::strtod(first, &end_ptr);
  if (end_ptr != last) {
    answer.ec = std::errc::invalid_argument;
  }

  return answer;
}

#define _GEOARROW_FROM_CHARS(first, last, out) from_chars_internal(first, last, out)
}


#endif

namespace geoarrow {

namespace {

class ParserException: public std::runtime_error {
public:
  ParserException(std::string expected, std::string found, std::string context):
  std::runtime_error(makeError(expected, found, context)),
    expected(expected), found(found), context(context) {}

  std::string expected;
  std::string found;
  std::string context;

  static std::string makeError(std::string expected, std::string found, std::string context = "") {
    std::stringstream stream;
    stream << "Expected " << expected << " but found " << found << context;
    return stream.str().c_str();
  }
};


// The Parser class provides the basic helpers needed to parse simple
// text formats like well-known text. It is not intended to be the pinnacle
// of speed or elegance, but does a good job at providing reasonable error
// messages The intended usage is to subclass the Parser for a particular
// format.
class Parser {
public:
  Parser(): length(0), offset(0),
    whitespace(" \r\n\t"), sep(" \r\n\t") {}

  void setBuffer(const char* data, int64_t size) {
    this->offset = 0;
    this->length = size;
    this->str = data;
  }

  const char* setWhitespace(const char* whitespace) {
    const char* previous_whitespace = this->whitespace;
    this->whitespace = whitespace;
    return previous_whitespace;
  }

  const char* setSeparators(const char* separators) {
    const char* previous_sep = this->sep;
    this->sep = separators;
    return previous_sep;
  }

  int64_t charsLeftInBuffer() {
    return this->length - this->offset;
  }

  bool checkBuffer(int n_chars) {
    return (this->charsLeftInBuffer() - n_chars) >= 0;
  }

  bool finished() {
    return !(this->checkBuffer(1));
  }

  void advance() {
    if (this->checkBuffer(1)) {
      this->offset++;
    }
  }

  // Returns the character at the cursor and advances the cursor by one
  char readChar() {
    char out = this->peekChar();
    this->advance();
    return out;
  }

  // Returns the character currently ahead of the cursor without advancing the cursor (skips whitespace)
  char peekChar() {
    this->skipWhitespace();
    if (this->checkBuffer(1)) {
      return this->str[this->offset];
    } else {
      return '\0';
    }
  }

  // Returns true if the next character is one of `chars`
  bool is(char c) {
    return c == this->peekChar();
  }

  // Returns true if the next character is one of `chars`
  bool isOneOf(const char* chars) {
    return strchr(chars, this->peekChar()) != nullptr;
  }

  // Returns true if the next character is most likely to be a number
  bool isNumber() {
    // complicated by nan and inf
    if (this->isOneOf("-nNiI.")) {
      std::string text = this->peekUntilSep();
      double out;
      auto result = _GEOARROW_FROM_CHARS(text.data(), text.data() + text.size(), out);
      return result.ec == std::errc();
    } else {
      return this->isOneOf("-0123456789");
    }
  }

  // Returns true if the next character is a letter
  bool isLetter() {
    char found = this->peekChar();
    return (found >= 'a' && found <= 'z') || (found >= 'A' && found <= 'Z');
  }

  std::string assertWord() {
    std::string text = this->peekUntilSep();
    if (!this->isLetter()) {
      this->error("a word", quote(text));
    }

    this->offset += text.size();
    return text;
  }

  // Returns the integer currently ahead of the cursor,
  // throwing an exception if whatever is ahead of the
  // cursor cannot be parsed into an integer
  long assertInteger() {
    std::string text = this->peekUntilSep();

    try {
      long out = std::stol(text);
      this->offset += text.size();
      return out;
    } catch (std::invalid_argument& e) {
      this->error("an integer", quote(text));
    }
  }

  // Returns the double currently ahead of the cursor,
  // throwing an exception if whatever is ahead of the
  // cursor cannot be parsed into a double. This will
  // accept "inf", "-inf", and "nan".
  double assertNumber() {
    std::string text = this->peekUntilSep();
    double out;
    auto result = _GEOARROW_FROM_CHARS(text.data(), text.data() + text.size(), out);

    if (result.ec != std::errc()) {
      this->error("a number", quote(text));
    } else {
      this->offset += text.size();
      return out;
    }
  }

  // Asserts that the character at the cursor is whitespace, and
  // returns a std::string of whitespace characters, advancing the
  // cursor to the end of the whitespace.
  void assertWhitespace() {
    if (!this->checkBuffer(1)) {
      this->error("whitespace", "end of input");
    }

    char found = this->str[this->offset];
    if (strchr(this->whitespace, found) == nullptr) {
      this->error("whitespace", quote(found));
    }

    this->skipWhitespace();
  }

  void assert_(char c) {
    char found = this->peekChar();
    if (found != c) {
      this->error(quote(c), quote(found));
    }
    this->advance();
  }

  // Asserts the that the character at the cursor is one of `chars`
  // and advances the cursor by one (throwing an exception otherwise).
  char assertOneOf(const char* chars) {
    char found = this->peekChar();

    if ((strlen(chars) > 0) && this->finished()) {
      this->error(expectedFromChars(chars), "end of input");
    } else if (strchr(chars, found) == nullptr) {
      this->error(expectedFromChars(chars), quote(this->peekUntilSep()));
    }

    this->advance();
    return found;
  }

  // Asserts that the cursor is at the end of the input
  void assertFinished() {
    this->assert_('\0');
  }

  // Returns the text between the cursor and the next separator,
  // which is defined to be whitespace or the following characters: =;,()
  // advancing the cursor. If we are at the end of the string, this will
  // return std::string("")
  std::string readUntilSep() {
    this->skipWhitespace();
    int64_t wordLen = peekUntil(this->sep);
    bool finished = this->finished();
    if (wordLen == 0 && !finished) {
      wordLen = 1;
    }
    std::string out(this->str + this->offset, wordLen);
    this->offset += wordLen;
    return out;
  }

  // Returns the text between the cursor and the next separator without advancing the cursor.
  std::string peekUntilSep() {
    this->skipWhitespace();
    int64_t wordLen = peekUntil(this->sep);
    return std::string(this->str + this->offset, wordLen);
  }

  // Advances the cursor past any whitespace, returning the number of characters skipped.
  int64_t skipWhitespace() {
    return this->skipChars(this->whitespace);
  }

  // Skips all of the characters in `chars`, returning the number of characters skipped.
  int64_t skipChars(const char* chars) {
    int64_t n_skipped = 0;
    bool found = false;

    while (!found && !this->finished()) {
      while (this->charsLeftInBuffer() > 0) {
        if (strchr(chars, this->str[this->offset])) {
          this->offset++;
          n_skipped++;
        } else {
          found = true;
          break;
        }
      }
    }

    return n_skipped;
  }

  // Returns the number of characters until one of `chars` is encountered,
  // which may be 0.
  int64_t peekUntil(const char* chars) {
    if (this->finished()) {
      return 0;
    }

    int64_t n_chars = -1;
    bool found = false;

    while (!found && this->checkBuffer(n_chars + 2)) {
      while ((this->offset + n_chars + 1) < this->length) {
        n_chars++;
        if (strchr(chars, this->str[this->offset + n_chars])) {
          found = true;
          break;
        }
      }
    }

    if (!found && (this->offset + n_chars + 1) == this->length) {
      n_chars++;
    }

    return n_chars;
  }

  [[ noreturn ]] void errorBefore(std::string expected, std::string found) {
    throw ParserException(expected, quote(found), this->errorContext(this->offset - found.size()));
  }

  [[noreturn]] void error(std::string expected, std::string found) {
    std::stringstream stream;
    stream << found;
    throw ParserException(expected, stream.str(), this->errorContext(this->offset));
  }

  [[noreturn]] void error(std::string expected) {
    throw ParserException(expected, quote(this->peekUntilSep()), this->errorContext(this->offset));
  }

  std::string errorContext(int64_t pos) {
    std::stringstream stream;
    stream << " at byte " << (this->offset + pos);
    return stream.str();
  }

private:
  const char* str;
  int64_t length;
  int64_t offset;
  const char* whitespace;
  const char* sep;

  static std::string expectedFromChars(const char* chars) {
    int64_t nChars = strlen(chars);
    std::stringstream stream;
    for (int64_t i = 0; i < nChars; i++) {
      if (i > 0) {
        stream << " or ";
      }
      stream << quote(chars[i]);
    }

    return stream.str();
  }

  static std::string quote(std::string input) {
    if (input.size() == 0) {
      return "end of input";
    } else {
      std::stringstream stream;
      stream << "'" << input << "'";
      return stream.str();
    }
  }

  static std::string quote(char input) {
    if (input == '\0') {
      return "end of input";
    } else {
      std::stringstream stream;
      stream << "'" << input << "'";
      return stream.str();
    }
  }
};

// The WKTParser is the Parser subclass with methods specific to well-known text.
class WKTParser: public Parser {
public:

  class WKTMeta {
  public:
    util::GeometryType geometry_type;
    util::Dimensions dimensions;
    bool is_empty;
  };

  WKTParser() {
    this->setSeparators(" \r\n\t,();=");
  }

  void assertGeometryMeta(WKTMeta* meta) {
    std::string geometry_type = this->assertWord();

    if (geometry_type == "SRID") {
      this->assert_('=');
      this->assertInteger();
      this->assert_(';');
      geometry_type = this->assertWord();
    }

    bool has_z = false;
    bool has_m = false;

    if (this->is('Z')) {
      this->assert_('Z');
      has_z = true;
    }

    if (this->is('M')) {
      this->assert_('M');
      has_m = true;
    }

    meta->geometry_type = this->geometry_typeFromString(geometry_type);
    meta->is_empty = this->isEMPTY();
    if (has_z && has_m) {
      meta->dimensions = util::Dimensions::XYZM;
    } else if (has_z) {
      meta->dimensions = util::Dimensions::XYZ;
    } else if (has_m) {
      meta->dimensions = util::Dimensions::XYM;
    } else {
      meta->dimensions = util::Dimensions::XY;
    }
  }

  util::GeometryType geometry_typeFromString(std::string geometry_type) {
    if (geometry_type == "POINT") {
      return util::GeometryType::POINT;
    } else if(geometry_type == "LINESTRING") {
      return util::GeometryType::LINESTRING;
    } else if(geometry_type == "POLYGON") {
      return util::GeometryType::POLYGON;
    } else if(geometry_type == "MULTIPOINT") {
      return util::GeometryType::MULTIPOINT;
    } else if(geometry_type == "MULTILINESTRING") {
      return util::GeometryType::MULTILINESTRING;
    } else if(geometry_type == "MULTIPOLYGON") {
      return util::GeometryType::MULTIPOLYGON;
    } else if(geometry_type == "GEOMETRYCOLLECTION") {
      return util::GeometryType::GEOMETRYCOLLECTION;
    } else {
      this->errorBefore("geometry type or 'SRID='", geometry_type);
    }
  }

  bool isEMPTY() {
    return this->peekUntilSep() == "EMPTY";
  }

  bool assertEMPTYOrOpen() {
    if (this->isLetter()) {
      std::string word = this->assertWord();
      if (word != "EMPTY") {
        this->errorBefore("'(' or 'EMPTY'", word);
      }

      return true;
    } else if (this->is('(')) {
      this->assert_('(');
      return false;
    } else {
      this->error("'(' or 'EMPTY'");
    }
  }
};

}

class WKTReader {
public:
  WKTReader() {}

  Handler::Result read_buffer(Handler* handler, const uint8_t* data, int64_t size) {
    s.setBuffer(reinterpret_cast<const char*>(data), size);
    try {
      Handler::Result result;
      HANDLE_OR_RETURN(readGeometryWithType(handler));
      s.assertFinished();
      return Handler::Result::CONTINUE;
    } catch(ParserException& e) {
      throw io::IOException("%s", e.what());
    }
  }

private:
  WKTParser s;
  WKTParser::WKTMeta meta_;
  double coord_[4];
  int32_t coord_size_;

  Handler::Result readGeometryWithType(Handler* handler) {
    util::Dimensions old_dim = meta_.dimensions;
    s.assertGeometryMeta(&meta_);

    if (meta_.dimensions != old_dim) {
      handler->new_dimensions(meta_.dimensions);
    }

    switch (meta_.dimensions) {
    case util::Dimensions::XYM:
    case util::Dimensions::XYZ:
      coord_size_ = 3;
      break;
    case util::Dimensions::XYZM:
      coord_size_ = 4;
      break;
    default:
      coord_size_ = 2;
    }

    Handler::Result result;
    if (meta_.is_empty) {
      HANDLE_OR_RETURN(handler->geom_start(meta_.geometry_type, 0));
    } else {
      HANDLE_OR_RETURN(handler->geom_start(meta_.geometry_type, -1));
    }


    switch (meta_.geometry_type) {

    case util::GeometryType::POINT:
      HANDLE_OR_RETURN(this->readPoint(handler));
      break;

    case util::GeometryType::LINESTRING:
      HANDLE_OR_RETURN(this->readLineString(handler));
      break;

    case util::GeometryType::POLYGON:
      HANDLE_OR_RETURN(this->readPolygon(handler));
      break;

    case util::GeometryType::MULTIPOINT:
      HANDLE_OR_RETURN(this->readMultiPoint(handler));
      break;

    case util::GeometryType::MULTILINESTRING:
      HANDLE_OR_RETURN(this->readMultiLineString(handler));
      break;

    case util::GeometryType::MULTIPOLYGON:
      HANDLE_OR_RETURN(this->readMultiPolygon(handler));
      break;

    case util::GeometryType::GEOMETRYCOLLECTION:
      HANDLE_OR_RETURN(this->readGeometryCollection(handler));
      break;

    default:
      throw std::runtime_error("Unknown geometry type"); // # nocov
    }

    return handler->geom_end();
  }

  Handler::Result readPoint(Handler* handler) {
    if (!s.assertEMPTYOrOpen()) {
      Handler::Result result;
      HANDLE_OR_RETURN(this->readPointCoordinate(handler));
      s.assert_(')');
    }

    return Handler::Result::CONTINUE;
  }

  Handler::Result readLineString(Handler* handler) {
    return this->readCoordinates(handler);
  }

  Handler::Result readPolygon(Handler* handler)  {
    return this->readLinearRings(handler);
  }

  Handler::Result readMultiPoint(Handler* handler) {
    if (s.assertEMPTYOrOpen()) {
      return Handler::Result::CONTINUE;
    }

    Handler::Result result;

    if (s.isNumber()) { // (0 0, 1 1)
      do {
        if (s.isEMPTY()) {
          HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POINT, 0));
          s.assertWord();
        } else {
          HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POINT, 1));
          HANDLE_OR_RETURN(this->readPointCoordinate(handler));
        }
        HANDLE_OR_RETURN(handler->geom_end());
      } while (s.assertOneOf(",)") != ')');

    } else { // ((0 0), (1 1))
      do {
        if (s.isEMPTY()) {
          HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POINT, 0));
        } else {
          HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POINT, 1));
        }
        HANDLE_OR_RETURN(this->readPoint(handler));
        HANDLE_OR_RETURN(handler->geom_end());
      } while (s.assertOneOf(",)") != ')');
    }

    return Handler::Result::CONTINUE;
  }

  Handler::Result readMultiLineString(Handler* handler) {
    if (s.assertEMPTYOrOpen()) {
      return Handler::Result::CONTINUE;
    }

    Handler::Result result;

    do {
      if (s.isEMPTY()) {
        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::LINESTRING, 0));
      } else {
        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::LINESTRING, -1));
      }
      HANDLE_OR_RETURN(this->readLineString(handler));
      HANDLE_OR_RETURN(handler->geom_end());
    } while (s.assertOneOf(",)") != ')');

    return Handler::Result::CONTINUE;
  }

  Handler::Result readMultiPolygon(Handler* handler) {
    if (s.assertEMPTYOrOpen()) {
      return Handler::Result::CONTINUE;
    }

    Handler::Result result;

    do {
      if (s.isEMPTY()) {
        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POLYGON, 0));
      } else {
        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POLYGON, -1));
      }
      HANDLE_OR_RETURN(this->readPolygon(handler));
      HANDLE_OR_RETURN(handler->geom_end());
    } while (s.assertOneOf(",)") != ')');

    return Handler::Result::CONTINUE;
  }

  Handler::Result readGeometryCollection(Handler* handler) {
    if (s.assertEMPTYOrOpen()) {
      return Handler::Result::CONTINUE;
    }

    Handler::Result result;

    do {
      HANDLE_OR_RETURN(this->readGeometryWithType(handler));
    } while (s.assertOneOf(",)") != ')');

    return Handler::Result::CONTINUE;
  }

  Handler::Result readLinearRings(Handler* handler) {
    if (s.assertEMPTYOrOpen()) {
      return Handler::Result::CONTINUE;
    }

    Handler::Result result;

    do {
      HANDLE_OR_RETURN(handler->ring_start(-1));
      HANDLE_OR_RETURN(this->readCoordinates(handler));
      HANDLE_OR_RETURN(handler->ring_end());
    } while (s.assertOneOf(",)") != ')');

    return Handler::Result::CONTINUE;
  }

  // Point coordinates are special in that there can only be one
  // coordinate (and reading more than one might cause errors since
  // writers are unlikely to expect a point geometry with many coordinates).
  // This assumes that `s` has already been checked for EMPTY or an opener
  // since this is different for POINT (...) and MULTIPOINT (.., ...)
  Handler::Result readPointCoordinate(Handler* handler) {
    this->readCoordinate();
    return handler->coords(coord_, 1, coord_size_);
  }

  Handler::Result readCoordinates(Handler* handler) {
    if (s.assertEMPTYOrOpen()) {
      return Handler::Result::CONTINUE;
    }

    Handler::Result result;
    do {
      this->readCoordinate();
      HANDLE_OR_RETURN(handler->coords(coord_, 1, coord_size_));
    } while (s.assertOneOf(",)") != ')');

    return Handler::Result::CONTINUE;
  }

  void readCoordinate() {
    coord_[0] = s.assertNumber();
    for (int i = 1; i < coord_size_; i++) {
      s.assertWhitespace();
      coord_[i] = s.assertNumber();
    }
  }
};

}

#undef _GEOARROW_FROM_CHARS
