<?php

namespace Nominatim;

require_once(CONST_TokenizerDir.'/tokenizer.php');

use Exception;

class Status
{
    protected $oDB;

    public function __construct(&$oDB)
    {
        $this->oDB =& $oDB;
    }

    public function status()
    {
        if (!$this->oDB) {
            throw new Exception('No database', 700);
        }

        try {
            $this->oDB->connect();
        } catch (\Nominatim\DatabaseError $e) {
            throw new Exception('Database connection failed', 700);
        }

        $oTokenizer = new \Nominatim\Tokenizer($this->oDB);
        $oTokenizer->checkStatus();
    }

    public function dataDate()
    {
        $sSQL = 'SELECT EXTRACT(EPOCH FROM lastimportdate) FROM import_status LIMIT 1';
        $iDataDateEpoch = $this->oDB->getOne($sSQL);

        if ($iDataDateEpoch === false) {
            throw Exception('Data date query failed '.$iDataDateEpoch->getMessage(), 705);
        }

        return $iDataDateEpoch;
    }

    public function databaseVersion()
    {
        $sSQL = 'SELECT value FROM nominatim_properties WHERE property = \'database_version\'';
        return $this->oDB->getOne($sSQL);
    }
}
