<?php
/**
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This file is part of Nominatim. (https://nominatim.org)
 *
 * Copyright (C) 2022 by the Nominatim developer community.
 * For a full list of authors see the git log.
 */

namespace Nominatim\Token;

/**
 * A house number token.
 */
class HouseNumber
{
    /// Database word id, if available.
    private $iId;
    /// Normalized house number.
    private $sToken;
    /// Match factor.
    private $iMatchRank;

    public function __construct($iId, $sToken, $iPenalty = 0)
    {
        $this->iId = $iId;
        $this->sToken = $sToken;
        $this->iMatchRank = 5 + $iPenalty;

        if (preg_match('/\\d/', $this->sToken) === 0
            || preg_match_all('/[^0-9 ]/', $this->sToken, $aMatches) > 3) {
            $this->iMatchRank += strlen($this->sToken) - 1;
        }

        if (empty($this->iId)) {
            $this->iMatchRank++;
        }

    }

    public function getId()
    {
        return $this->iId;
    }

    /**
     * Check if the token can be added to the given search.
     * Derive new searches by adding this token to an existing search.
     *
     * @param object  $oSearch      Partial search description derived so far.
     * @param object  $oPosition    Description of the token position within
                                    the query.
     *
     * @return True if the token is compatible with the search configuration
     *         given the position.
     */
    public function isExtendable($oSearch, $oPosition)
    {
        return !$oSearch->hasHousenumber()
               && !$oSearch->hasOperator(\Nominatim\Operator::POSTCODE)
               && $oPosition->maybePhrase('street');
    }

    /**
     * Derive new searches by adding this token to an existing search.
     *
     * @param object  $oSearch      Partial search description derived so far.
     * @param object  $oPosition    Description of the token position within
                                    the query.
     *
     * @return SearchDescription[] List of derived search descriptions.
     */
    public function extendSearch($oSearch, $oPosition)
    {
        $aNewSearches = array();

        // Should appear towards the beginning of the term.
        $iSearchCost = $oSearch->addressLength() + 4;

        if (!$oSearch->hasOperator(\Nominatim\Operator::NONE)) {
            $iSearchCost++;
        }

        if ($oSearch->hasPostcode()) {
            $iSearchCost += 2;
        }

        $oNewSearch = $oSearch->clone($this->iMatchRank, $iSearchCost);
        $oNewSearch->setHousenumber($this->sToken);
        $aNewSearches[] = $oNewSearch;

        // Housenumbers may appear in the name when the place has its own
        // address terms.
        if ($this->iId !== null
            && ($oSearch->getNamePhrase() >= 0 || !$oSearch->hasName())
            && !$oSearch->hasAddress()
        ) {
            $oNewSearch = $oSearch->clone($this->iMatchRank, $iSearchCost + 1);
            $oNewSearch->setHousenumberAsName($this->iId);

            $aNewSearches[] = $oNewSearch;
        }

        return $aNewSearches;
    }


    public function debugInfo()
    {
        return array(
                'ID' => $this->iId,
                'Type' => 'house number',
                'Rank' => $this->iMatchRank,
                'Info' => array('nr' => $this->sToken)
               );
    }

    public function debugCode()
    {
        return 'H';
    }
}
