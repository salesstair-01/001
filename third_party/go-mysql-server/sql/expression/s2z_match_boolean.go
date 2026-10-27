// s2z patch: MATCH ... AGAINST (... IN BOOLEAN MODE).

package expression

import (
	"fmt"
	"strings"
	"unicode"
)

// booleanMatch evaluates a boolean-mode search on a row's text, as InnoDB
// does: +word must occur, -word must not, a plain word may; with no +
// term, one of the plain words must occur. word* matches words that start
// with word, "a phrase" the words in order, ( ... ) groups terms; ~, < and
// > only change relevance in MySQL and are accepted. Search words shorter
// than three characters, and InnoDB's stopwords, are ignored. The result is
// the number of positive terms that matched (MySQL's relevance differs), 0
// for no match.
func booleanMatch(query string, docs []interface{}) (float32, error) {
	var words []string
	for _, d := range docs {
		var s string
		switch v := d.(type) {
		case nil:
			continue
		case string:
			s = v
		case []byte:
			s = string(v)
		default:
			s = fmt.Sprint(v)
		}
		words = append(words, splitWords(s)...)
	}
	p := &boolParser{s: query}
	g := p.group(false)
	score, ok := g.eval(words)
	if !ok {
		return 0, nil
	}
	return float32(score), nil
}

type boolTerm struct {
	op     byte // '+', '-' or 0
	word   string
	prefix bool
	phrase []string
	group  *boolGroup
}

type boolGroup struct{ terms []boolTerm }

func (g *boolGroup) eval(doc []string) (int, bool) {
	score, required, optional, optHit := 0, false, false, false
	for _, t := range g.terms {
		hit, s := t.match(doc)
		switch t.op {
		case '-':
			if hit {
				return 0, false
			}
		case '+':
			required = true
			if !hit {
				return 0, false
			}
			score += s
		default:
			optional = true
			if hit {
				optHit = true
				score += s
			}
		}
	}
	if !required && (!optional || !optHit) {
		return 0, false
	}
	return score, true
}

func (t boolTerm) match(doc []string) (bool, int) {
	switch {
	case t.group != nil:
		s, ok := t.group.eval(doc)
		return ok, s
	case t.phrase != nil:
		if len(t.phrase) == 0 {
			return false, 0
		}
		n := 0
		for i := 0; i+len(t.phrase) <= len(doc); i++ {
			all := true
			for j, w := range t.phrase {
				if doc[i+j] != w {
					all = false
					break
				}
			}
			if all {
				n++
			}
		}
		return n > 0, n
	}
	n := 0
	for _, w := range doc {
		if w == t.word || (t.prefix && strings.HasPrefix(w, t.word)) {
			n++
		}
	}
	return n > 0, min(n, 1)
}

type boolParser struct {
	s string
	i int
}

func (p *boolParser) group(nested bool) *boolGroup {
	g := &boolGroup{}
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			p.i++
			continue
		case c == ')':
			p.i++
			if nested {
				return g
			}
			continue
		}
		var op byte
		for p.i < len(p.s) && strings.IndexByte("+-~<>", p.s[p.i]) >= 0 {
			if p.s[p.i] == '+' || p.s[p.i] == '-' {
				op = p.s[p.i]
			}
			p.i++
		}
		if p.i >= len(p.s) {
			break
		}
		switch p.s[p.i] {
		case '(':
			p.i++
			g.terms = append(g.terms, boolTerm{op: op, group: p.group(true)})
		case '"':
			p.i++
			end := strings.IndexByte(p.s[p.i:], '"')
			text := p.s[p.i:]
			if end >= 0 {
				text = p.s[p.i : p.i+end]
				p.i += end + 1
			} else {
				p.i = len(p.s)
			}
			g.terms = append(g.terms, boolTerm{op: op, phrase: splitWords(text)})
		default:
			start := p.i
			for p.i < len(p.s) && isWordChar(rune(p.s[p.i])) {
				p.i++
			}
			word := strings.ToLower(p.s[start:p.i])
			prefix := false
			if p.i < len(p.s) && p.s[p.i] == '*' {
				prefix = true
				p.i++
			}
			if start == p.i { // a character that is neither: skip it
				p.i++
				continue
			}
			if word == "" || (!prefix && (len([]rune(word)) < 3 || innodbStopwords[word])) {
				continue
			}
			g.terms = append(g.terms, boolTerm{op: op, word: word, prefix: prefix})
		}
	}
	return g
}

func isWordChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '\'' || r >= 0x80
}

// splitWords is a text's words, lower case.
func splitWords(s string) []string {
	var out []string
	for _, w := range strings.FieldsFunc(s, func(r rune) bool { return !isWordChar(r) }) {
		w = strings.Trim(w, "'")
		if w != "" {
			out = append(out, strings.ToLower(w))
		}
	}
	return out
}

// innodbStopwords is InnoDB's default stopword list.
var innodbStopwords = map[string]bool{
	"a": true, "about": true, "an": true, "are": true, "as": true, "at": true, "be": true, "by": true,
	"com": true, "de": true, "en": true, "for": true, "from": true, "how": true, "i": true, "in": true,
	"is": true, "it": true, "la": true, "of": true, "on": true, "or": true, "that": true, "the": true,
	"this": true, "to": true, "was": true, "what": true, "when": true, "where": true, "who": true,
	"will": true, "with": true, "und": true, "www": true,
}
