
#ifndef WK_BUFFERED_READER_H_INCLUDED
#define WK_BUFFERED_READER_H_INCLUDED

#include "fast_float/fast_float.h"
#include <cstring>
#include <sstream>
#include <cstdlib>

class BufferedParserException: public std::runtime_error {
public:
  BufferedParserException(std::string expected, std::string found, std::string context):
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

// The SimpleBufferSource is a wrapper around an in-memory buffer of characters.
// The BufferedParser classes below template along an object with a fill_buffer()
// method with the same signature as this one.
class SimpleBufferSource {
public:
  SimpleBufferSource(): str(nullptr), size(0), offset(0) {}

  void set_buffer(const char* str, int64_t size) {
    this->str = str;
    this->size = size;
    this->offset = 0;
  }

  int64_t fill_buffer(char* buffer, int64_t max_size) {
    int64_t copy_size = std::min<int64_t>(this->size - this->offset, max_size);
    if (copy_size > 0) {
      memcpy(buffer, this->str + this->offset, copy_size);
      this->offset += copy_size;
      return copy_size;
    } else {
      return 0;
    }
  }

private:
  const char* str;
  int64_t size;
  int64_t offset;
};


// The BufferedParser class provides the basic helpers needed to parse simple
// text formats like well-known text. It is not intended to be the pinnacle
// of speed or elegance, but does a good job at providing reasonable error
// messages and has the important feature that it does not need the text
// that it's parsing to be fully in-memory. The intended usage is to subclass
// the BufferedParser for a particular format.
template <class SourceType, int64_t buffer_length>
class BufferedParser {
public:
  BufferedParser(): length(0), offset(0), source_offset(0),
    whitespace(" \r\n\t"), sep(" \r\n\t"), source(nullptr) {}

  void setSource(SimpleBufferSource* source) {
    this->source = source;
    this->offset = 0;
    this->length = 0;
    this->source_offset = 0;
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
    int64_t chars_to_keep = this->charsLeftInBuffer();
    if ((chars_to_keep - n_chars) >= 0) {
        return true;
    }

    if (n_chars >= buffer_length) {
      std::stringstream stream;
      stream << "a value with fewer than " << buffer_length << " characters";
      throw BufferedParserException(stream.str(), "a longer value", "");
    }

    if (this->source == nullptr) {
      return false;
    }

    if (chars_to_keep > 0) {
      memmove(this->str, this->str + this->offset, chars_to_keep);
    }

    int64_t new_chars = this->source->fill_buffer(this->str + chars_to_keep, buffer_length - chars_to_keep);
    if (new_chars == 0) {
      this->source = nullptr;
    }

    this->source_offset += new_chars;
    this->offset = 0;
    this->length = chars_to_keep + new_chars;
    return n_chars <= this->length;
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
      auto result = fast_float::from_chars(text.data(), text.data() + text.size(), out);
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
    auto result = fast_float::from_chars(text.data(), text.data() + text.size(), out);

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
    throw BufferedParserException(expected, quote(found), this->errorContext(this->offset - found.size()));
  }

  [[noreturn]] void error(std::string expected, std::string found) {
    std::stringstream stream;
    stream << found;
    throw BufferedParserException(expected, stream.str(), this->errorContext(this->offset));
  }

  [[noreturn]] void error(std::string expected) {
    throw BufferedParserException(expected, quote(this->peekUntilSep()), this->errorContext(this->offset));
  }

  std::string errorContext(int64_t pos) {
    std::stringstream stream;
    stream << " at byte " << (this->source_offset - this->length + pos);
    return stream.str();
  }

private:
  char str[buffer_length];
  int64_t length;
  int64_t offset;
  int64_t source_offset;
  const char* whitespace;
  const char* sep;
  SimpleBufferSource* source;

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

#endif
