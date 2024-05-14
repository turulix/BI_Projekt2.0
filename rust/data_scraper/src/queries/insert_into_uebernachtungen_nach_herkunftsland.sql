INSERT INTO original_data.uebernachtungen_nach_herkunftsland (herkunftsregion, jahr, monat, ankuenfte_anzahl,
                                                              ankuenfte_veraenderung_zum_vorjahreszeitraum_prozent,
                                                              uebernachtungen_anzahl,
                                                              uebernachtungen_veraenderung_zum_vorjahreszeitraum_prozent,
                                                              durchsch_aufenthaltsdauer_tage)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
