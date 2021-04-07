<?php

namespace Nominatim;

class Tokenizer {
    private $oDB;

    public $oNormalizer = null;

    public function __construct(&$oDB)
    {
        $this->oDB =& $oDB;
        $this->oNormalizer = \Transliterator::createFromRules(CONST_Term_Normalization_Rules);
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
        $aSearchWords = $this->oDB->getAll($sSQL, array(':term' => $sTerm), 'XX');

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
};
