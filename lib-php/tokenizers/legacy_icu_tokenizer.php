<?php

namespace Nominatim;

class Tokenizer {
    private $oDB;

    private $oNormalizer = null;
    private $aCountryRestriction = null;

    public function __construct(&$oDB)
    {
        $this->oDB =& $oDB;
        $this->oNormalizer = \Transliterator::createFromRules(CONST_Term_Normalization_Rules);
    }


    public function setCountryRestriction($aCountries)
    {
        $this->aCountryRestriction = $aCountries;
    }


    public function normalizeString($sTerm)
    {
        if ($this->oNormalizer === null) {
            return $sTerm;
        }

        return $this->oNormalizer->transliterate($sTerm);
    }


    public function tokensForSpecialTerm($sTerm)
    {
        $aResults = array();

        $sSQL = 'SELECT word_id, class, type FROM word ';
        $sSQL .= '   WHERE word_token = \' \' || make_standard_name(:term)';
        $sSQL .= '   AND class is not null AND class not in (\'place\')';

        Debug::printVar('Term', $sTerm);
        Debug::printSQL($sSQL);
        $aSearchWords = $this->oDB->getAll($sSQL, array(':term' => $sTerm));

        Debug::printVar('Results', $aSearchWords);

        foreach ($aSearchWords as $aSearchTerm) {
            $aResults[] = new \Nominatim\Token\SpecialTerm(
                              $aSearchTerm['word_id'],
                              $aSearchTerm['class'],
                              $aSearchTerm['type'],
                              \Nominatim\Operator::TYPE
            );
        }

        Debug::printVar('Special term tokens', $aResults);

        return $aResults;
    }


    public function extractTokensFromPhrases(&$aPhrases) {

        // First get the normalized version of all phrases
        $sNormQuery = '';
        $sSQL = 'SELECT ';
        $aParams = array();
        foreach ($aPhrases as $iPhrase => $oPhrase) {
            $sNormQuery .= ','.$this->normalizeString($oPhrase->getPhrase());
            $sSQL .= 'make_standard_name(:' .$iPhrase.') as p'.$iPhrase.',';
            $aParams[':'.$iPhrase] = $oPhrase->getPhrase();
        }
        $sSQL = substr($sSQL, 0, -1);

        Debug::printSQL($sSQL);
        Debug::printVar('SQL parameters', $aParams);

        $aNormPhrases = $this->oDB->getRow($sSQL, $aParams);

        Debug::printVar('SQL result', $aNormPhrases);

        // now compute all possible tokens
        $aWordLists = array();
        $aTokens = array();
        foreach ($aNormPhrases as $sTitle => $sPhrase) {
            if (strlen($sPhrase) > 0) {
                $aWords = explode(' ', $sPhrase);
                Tokenizer::addTokens($aTokens, $aWords);
                $aWordLists[] = $aWords;
            } else {
                $aWordLists[] = array();
            }
        }

        Debug::printVar('Tokens', $aTokens);
        Debug::printVar('WordLists', $aWordLists);

        $oValidTokens = $this->computeValidTokens($aTokens, $sNormQuery);

        foreach ($aPhrases as $iPhrase => $oPhrase) {
            $oPhrase->computeWordSets($aWordLists[$iPhrase], $oValidTokens);
        }

        return $oValidTokens;
    }


    private function computeValidTokens($aTokens, $sNormQuery)
    {
        $oValidTokens = new TokenList();

        if (!empty($aTokens)) {
            $this->addTokensFromDB($oValidTokens, $aTokens, $sNormQuery);

            // Try more interpretations for Tokens that could not be matched.
            foreach ($aTokens as $sToken) {
                if ($sToken[0] == ' ' && !$oValidTokens->contains($sToken)) {
                    if (preg_match('/^ ([0-9]{5}) [0-9]{4}$/', $sToken, $aData)) {
                        // US ZIP+4 codes - merge in the 5-digit ZIP code
                        $oValidTokens->addToken(
                            $sToken,
                            new Token\Postcode(null, $aData[1], 'us')
                        );
                    } elseif (preg_match('/^ [0-9]+$/', $sToken)) {
                        // Unknown single word token with a number.
                        // Assume it is a house number.
                        $oValidTokens->addToken(
                            $sToken,
                            new Token\HouseNumber(null, trim($sToken))
                        );
                    }
                }
            }
        }

        return $oValidTokens;
    }


    private function addTokensFromDB(&$oValidTokens, $aTokens, $sNormQuery)
    {
        // Check which tokens we have, get the ID numbers
        $sSQL = 'SELECT word_id, word_token, word, class, type, country_code,';
        $sSQL .= ' operator, coalesce(search_name_count, 0) as count';
        $sSQL .= ' FROM word WHERE word_token in (';
        $sSQL .= join(',', $this->oDB->getDBQuotedList($aTokens)).')';

        Debug::printSQL($sSQL);

        $aDBWords = $this->oDB->getAll($sSQL, null, 'Could not get word tokens.');

        foreach ($aDBWords as $aWord) {
            $oToken = null;
            $iId = (int) $aWord['word_id'];

            if ($aWord['class']) {
                // Special terms need to appear in their normalized form.
                // (postcodes are not normalized in the word table)
                $sNormWord = $this->normalizeString($aWord['word']);
                if ($aWord['word'] && strpos($sNormQuery, $sNormWord) === false) {
                    continue;
                }

                if ($aWord['class'] == 'place' && $aWord['type'] == 'house') {
                    $oToken = new Token\HouseNumber($iId, trim($aWord['word_token']));
                } elseif ($aWord['class'] == 'place' && $aWord['type'] == 'postcode') {
                    if ($aWord['word']
                        && pg_escape_string($aWord['word']) == $aWord['word']
                    ) {
                        $oToken = new Token\Postcode(
                            $iId,
                            $aWord['word'],
                            $aWord['country_code']
                        );
                    }
                } else {
                    // near and in operator the same at the moment
                    $oToken = new Token\SpecialTerm(
                        $iId,
                        $aWord['class'],
                        $aWord['type'],
                        $aWord['operator'] ? Operator::NEAR : Operator::NONE
                    );
                }
            } elseif ($aWord['country_code']) {
                // Filter country tokens that do not match restricted countries.
                if (!$this->aCountryRestriction
                    || in_array($aWord['country_code'], $this->aCountryRestriction)
                ) {
                    $oToken = new Token\Country($iId, $aWord['country_code']);
                }
            } else {
                $oToken = new Token\Word(
                    $iId,
                    $aWord['word_token'][0] != ' ',
                    (int) $aWord['count'],
                    substr_count($aWord['word_token'], ' ')
                );
            }

            if ($oToken) {
                $oValidTokens->addToken($aWord['word_token'], $oToken);
            }
        }
    }


    /**
     * Add the tokens from this phrase to the given list of tokens.
     *
     * @param string[] $aTokens List of tokens to append.
     *
     * @return void
     */
    private static function addTokens(&$aTokens, $aWords)
    {
        $iNumWords = count($aWords);

        for ($i = 0; $i < $iNumWords; $i++) {
            $sPhrase = $aWords[$i];
            $aTokens[' '.$sPhrase] = ' '.$sPhrase;
            $aTokens[$sPhrase] = $sPhrase;

            for ($j = $i + 1; $j < $iNumWords; $j++) {
                $sPhrase .= ' '.$aWords[$j];
                $aTokens[' '.$sPhrase] = ' '.$sPhrase;
                $aTokens[$sPhrase] = $sPhrase;
            }
        }
    }
};
