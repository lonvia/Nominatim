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
 * A standard word token.
 */
class Partial
{
    /// Database word id, if applicable.
    private $iId;
    /// Number of appearances in the database.
    private $iSearchNameCount;
    /// Match factor.
    private $iMatchRank;

    public function __construct($iId, $sToken, $iSearchNameCount, $iPenalty = 0)
    {
        $this->iId = $iId;
        $this->iSearchNameCount = $iSearchNameCount;
        $this->iMatchRank = 5 + $iPenalty;

        if (preg_match('#^[0-9 ]+$#', $sToken)) {
            $this->iMatchRank += 2;
        }

        if ($iSearchNameCount >= CONST_Max_Word_Frequency) {
            $this->iMatchRank += 1;
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
        return !$oPosition->isPhrase('country');
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

        // Partial token in Address.
        if (($oPosition->isPhrase('') || !$oPosition->isFirstPhrase())
            && $oSearch->hasName()
        ) {
            $oNewSearch = $oSearch->clone($this->iMatchRank, 5);
            $oNewSearch->addAddressToken(
                $this->iId,
                $this->iSearchNameCount < CONST_Max_Word_Frequency
            );

            $aNewSearches[] = $oNewSearch;
        }

        // Partial token in Name.
        if ((!$oSearch->hasPostcode() && !$oSearch->hasAddress())
            && (!$oSearch->hasName(true)
                || $oSearch->getNamePhrase() == $oPosition->getPhrase())
        ) {
            $iSearchCost = 5;
            if (!$oSearch->hasName(true)) {
                $iSearchCost += 1;
            }

            $oNewSearch = $oSearch->clone($this->iMatchRank, $iSearchCost);
            $oNewSearch->addPartialNameToken(
                $this->iId,
                $this->iSearchNameCount < CONST_Max_Word_Frequency,
                $this->iSearchNameCount > CONST_Search_NameOnlySearchFrequencyThreshold,
                $oPosition->getPhrase()
            );

            $aNewSearches[] = $oNewSearch;
        }

        return $aNewSearches;
    }


    public function debugInfo()
    {
        return array(
                'ID' => $this->iId,
                'Type' => 'partial',
                'Rank' => $this->iMatchRank,
                'Info' => array(
                           'count' => $this->iSearchNameCount
                          )
               );
    }

    public function debugCode()
    {
        return 'w';
    }
}
