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
 * A country token.
 */
class Country
{
    /// Database word id, if available.
    private $iId;
    /// Two-letter country code (lower-cased).
    private $sCountryCode;
    /// Match factor.
    private $iMatchRank;

    public function __construct($iId, $sCountryCode, $iPenalty = 0)
    {
        $this->iId = $iId;
        $this->sCountryCode = $sCountryCode;
        $this->iMatchRank = 5 + $iPenalty;
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
        return !$oSearch->hasCountry()
               && $oPosition->maybePhrase('country')
               && $oSearch->getContext()->isCountryApplicable($this->sCountryCode);
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
        $oNewSearch = $oSearch->clone($this->iMatchRank, $oPosition->isLastToken() ? 1 : 8);
        $oNewSearch->setCountry($this->sCountryCode);

        return array($oNewSearch);
    }

    public function debugInfo()
    {
        return array(
                'ID' => $this->iId,
                'Type' => 'country',
                'Rank' => $this->iMatchRank,
                'Info' => $this->sCountryCode
               );
    }

    public function debugCode()
    {
        return 'C';
    }
}
