def query_string1():
    return ("Get the registrant name",
            "Get the registrant name for the filing with adsh '0000059255-24-000004'", """
SELECT s.name, t.tag, t.value
FROM SUB s 
LEFT JOIN TXT t ON s.adsh = t.adsh AND t.tag = 'EntityRegistrantName'
WHERE s.adsh='0000059255-24-000004'
""")

def query_string2():
    return ("What direct payments to shareholders were reported in 2022? How much were they per common share?",
            "What direct payments to shareholders were reported in 2022 in the filing 0000002178-24-000035? How much were they per common share?","""
WITH filing AS ( 
    SELECT adsh 
    FROM SUB    
    WHERE adsh='0000002178-24-000035'
), 

used_tags AS ( 
    SELECT txt.tag, txt.value, txt.ddate 
    FROM TXT 
    JOIN filing as f 
    WHERE txt.adsh=f.adsh 
    AND ddate BETWEEN 20220101 AND 20221231

    UNION 

    SELECT num.tag, num.value, num.ddate 
    FROM NUM 
    JOIN filing as f 
    WHERE num.adsh=f.adsh 
    AND ddate BETWEEN 20220101 AND 20221231
) 

SELECT tag, value, ddate
FROM used_tags 
WHERE tag LIKE '%Dividend%'
ORDER BY tag ASC
    """)

def query_string3():
    return ("List all the figures representing capital investments in 2023.",
            "For the filing 0001437749-24-009833 list all the figures representing capital investments in 2023.", """
SELECT adsh, tag, version, ddate, qtrs, uom, value
FROM NUM
WHERE adsh='0001437749-24-009833'
AND tag IN ('AvailableForSaleSecuritiesDebtSecurities','InvestmentsFairValueDisclosure', 'InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures', 'PaymentsToAcquireOtherInvestments', 'PaymentsToAcquireRealEstateHeldForInvestment', 'OtherInvestments', 'OtherShortTermInvestments')
AND ddate BETWEEN 20230101 AND 20231231
ORDER BY tag ASC
    """)

# We cannot return all the tags about cash, so write a custom one knowing no data is present
def query_string4():
    return ("Is there any information about cash holdings?'",
            "Is there any information about cash holdings for the filing 0001521945-24-000010?", """
SELECT value
FROM TXT
WHERE adsh='0001521945-24-000010'
AND value LIKE '%INVESTMENT OBJECTIVE AND STRATEGIES%'
    """)


def query_string5():
    return ("Return all the available geographical information.",
            "Return all the available geographical information related to the 'FEDERAL HOME LOAN BANK OF CINCINNATI' whose accesion number is 0001326771-24-000013.", """
SELECT tag, value
FROM TXT 
WHERE adsh='0001326771-24-000013'
AND (tag='CityAreaCode' OR tag='EntityAddressAddressLine1' OR tag='EntityAddressCityOrTown' OR tag='EntityAddressPostalZipCode' OR tag='EntityAddressStateOrProvince' OR tag='EntityIncorporationStateCountryCode')
    """)


def query_string6():
    return ("Does the filing include goodwill in the calculation of assets?",
            "Does the filing PARTY CITY HOLDCO INC. (with adsh 0000950170-24-037962) include goodwill in the calculation of assets?", """
SELECT * 
FROM CAL  
WHERE adsh='0000950170-24-037962'
AND ctag='Goodwill'
AND ptag='Assets'
    """)


def query_string7():
    return ("What figures are included in the calculation of assets?",
            "In the filing 0000950170-24-037962 what figures are included in the calculation of assets?", """
SELECT ctag 
FROM CAL 
WHERE adsh='0000950170-24-037962'
AND ptag='Assets' 
    """)

def query_string8():
    return ("What kind of filing is this?",
            "What kind of filing is the one with adsh 0001108426-24-000015?", """
SELECT form, NULL as tag
FROM SUB 
WHERE adsh='0001108426-24-000015'

UNION ALL

SELECT value, tag
FROM TXT
WHERE adsh='0001108426-24-000015'
AND tag='DocumentType'
    """)

def query_string9():
    return ("What is the latest debt value reported?",
            "What is the latest debt value reported by the filing 0001521945-24-000010?", """
WITH chronological_debts AS (
    SELECT tag, ddate, value
    FROM NUM 
    WHERE adsh='0001521945-24-000010' 
    AND tag='SeniorSecuritiesAmt'
    ORDER BY ddate DESC
)   
SELECT tag, ddate, value
FROM chronological_debts
LIMIT 1
    """)

def query_string10():
    return ("Check if the report data lies in 2024",
            "Check if the report data of the filing of RED RIVER BANCSHARES INC with unique code 0001071236-24-000011 lies in 2024", """
SELECT period, filed, accepted
FROM SUB 
WHERE adsh='0001071236-24-000011' 
AND name='RED RIVER BANCSHARES INC' 
AND period BETWEEN 20240101 AND 20250101 
    """)

def query_string11():
    return ("Are there any disclaimers about the assumptions or estimates reported?",
            "Are there any disclaimers about the assumptions or estimates reported in the filing 0000950170-24-031457?", """
SELECT tag, version, value 
FROM TXT 
WHERE adsh='0000950170-24-031457' 
AND tag='UseOfEstimates'
AND version='us-gaap/2023'
AND ddate=20240131
AND qtrs=3
AND iprx=0

UNION ALL

SELECT tag, version, value
FROM TXT
WHERE adsh='0000950170-24-031457'
AND tag='SignificantAccountingPoliciesTextBlock'
    """)

def query_string12():
    return ("Are there any negative values reported in 2022 associated to stockholders?'",
            "Are there any negative values reported in 2022 for the filing with adsh '0001628280-24-013372 associated to stockholders?'", """
SELECT tag, ddate, value
FROM NUM
WHERE adsh = '0001628280-24-013372'
  AND ddate BETWEEN 20220101 AND 20221231
  AND value < 0
  AND (tag LIKE '%Stockholders%' OR tag = 'RetainedEarningsAccumulatedDeficit');    """) # "RetainedEarningsAccumulatedDeficit" refers to the accumulated profits or losses a company has over its lifetime[1]. It represents the portion of a company's net income that has been retained for reinvestment in the business rather than distributed to shareholders as dividends. It reduces stockholders' equity when negative.

def query_string13():
    return ("What's the address of the submitting entity?",
            "What's the address of the submitting entity of filing 0000927653-24-000018", """
SELECT s.countryba, s.stprba, s.cityba, s.zipba, s.bas1,
       addr.value, city.value,
       zip.value, state.value, country.value
FROM SUB s
LEFT JOIN TXT addr ON addr.adsh = s.adsh AND addr.tag = 'EntityAddressAddressLine1'
LEFT JOIN TXT city ON city.adsh = s.adsh AND city.tag = 'EntityAddressCityOrTown'
LEFT JOIN TXT zip ON zip.adsh = s.adsh AND zip.tag = 'EntityAddressPostalZipCode'
LEFT JOIN TXT state ON state.adsh = s.adsh AND state.tag = 'EntityAddressStateOrProvince'
LEFT JOIN TXT country ON country.adsh = s.adsh AND country.tag = 'EntityIncorporationStateCountryCode'
WHERE s.adsh='0000927653-24-000018'
    """)

def query_string14():
    return ("What accounting standard version does the filing use in the text disclosures in 2018?",
            "What accounting standard version does the filing 0000950170-24-037962 use in the text disclosures in 2018?", """
SELECT ddate, tag, version, value 
FROM TXT
WHERE adsh='0000950170-24-037962'
AND ddate BETWEEN 20180101 AND 20181231
    """)

def query_string15():
    return ("What is the history of gross profit values?",
            "What is the history of gross profit values of filing 0000038725-24-000038?", """
SELECT tag, version, ddate, value, uom
FROM NUM
WHERE adsh='0000038725-24-000038'
AND tag='GrossProfit'
ORDER BY ddate ASC
    """)


def query_string16():
    return ("What does 'ContractWithCustomerLiability' mean?",
            "What does 'ContractWithCustomerLiability' mean, considering it has been used by the filing 0000039311-24-000035?", """
SELECT tag, version, doc
FROM TAG 
WHERE tag='ContractWithCustomerLiability'
and version='us-gaap/2023'
    """)

def query_string17():
    return ("Are intagible assets used in any calculation relationships? Return all the records",
            "Are intagible assets reported by the filing 0001058623-24-000025 used in any calculation relationships? Return all the records", """
SELECT * 
FROM CAL  
WHERE adsh='0001058623-24-000025'
AND (ctag LIKE '%IntangibleAssets%' OR ptag LIKE '%IntangibleAssets%') 
""")

def query_string18():
    return ("Show the presentation characteristics of the balance sheet section",
            "Show the presentation characteristics of the balance sheet section of the filing 0001999371-24-003926", """
SELECT * 
FROM PRE 
WHERE adsh='0001999371-24-003926' 
AND stmt='BS'
    """)

def query_string19():
    return ("How much cash did the company have available for this filing?",
            "How much cash did the company have available for this filing 0002008589-24-000010?", """
WITH chronological_cash AS (
SELECT tag, version, ddate, iprx, value, uom
FROM NUM 
WHERE adsh='0002008589-24-000010' 
AND tag='Cash'
ORDER BY ddate DESC, iprx DESC  
LIMIT 1
)
SELECT tag, version, ddate, value, uom
FROM chronological_cash

UNION ALL

SELECT tag, version, ddate, value, NULL as uom
FROM TXT
WHERE adsh='0002008589-24-000010'
AND tag='CashAndCashEquivalentsPolicyTextBlock'
    """)


def query_string20():
    return ("Who was the auditor of Dakota Gold Corp in 2023?",
            "Who was the auditor of Dakota Gold Corp in 2023 (filing 0001062993-24-007319)", """
SELECT tag, value, ddate
FROM TXT
WHERE adsh = '0001062993-24-007319'
AND tag = 'AuditorName'
    """)
def query_string21():
    return ("Is the filing an annual non-foreign report?",
            "Is the filing 0001477932-24-001283 an annual non-foreign report?", """
SELECT s.form, t.tag, t.value
FROM SUB s
LEFT JOIN TXT t ON t.adsh = s.adsh AND t.tag = 'DocumentAnnualReport'
WHERE s.adsh='0001477932-24-001283' 
AND s.form='10-K' 
    """)

def query_string22():
    return ("Show where in the statement the current prepaid expense is presented",
            "Show where in the statement the current prepaid expense is presented in the filing 0000002969-24-000010", """
SELECT tag, version, plabel, line, stmt
FROM PRE 
WHERE adsh='0000002969-24-000010'
AND tag='PrepaidExpenseCurrent' 
AND stmt='BS'
    """)


def query_string23():
    return ("Show the components of the current Assets",
            "For the filing 0001477932-24-001599, show the components of the current Assets", """
SELECT ctag
FROM CAL
WHERE adsh='0001477932-24-001599'
AND ptag='CurrentAssets'
    """)


def query_string24():
    return ("Which custom financial tags are presented in this filing?",
            "Which custom financial tags are presented in this filing? (adsh 0001104659-24-033311)", """
SELECT DISTINCT t.tag, t.version, t.doc, t.custom
FROM TAG t
WHERE t.custom=1
AND (
    t.tag IN (SELECT DISTINCT tag FROM NUM WHERE adsh='0001104659-24-033311')
    OR
    t.tag IN (SELECT DISTINCT tag FROM TXT WHERE adsh='0001104659-24-033311')
)
ORDER BY t.tag
    """)

def query_string25():
    return ("Explain the balance sheet called 'AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment'",
            "Given the filing 0000002969-24-000010, explain the balance sheet called 'AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment'", """
SELECT tag, version, tlabel, doc
FROM TAG
WHERE tag='AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment'
AND version='us-gaap/2023'
    """)

def query_string26():
    return ("Is there any numerical information about occupancy costs after all the expenses?",
            "Considering the filing 0001104659-24-034403, is there any numerical information about occupancy costs after all the expenses?", """
SELECT tag, ddate, value, uom
FROM NUM
WHERE adsh='0001104659-24-034403'
AND tag='OccupancyNet'
    """)

def query_string27():
    return ("What were the latest figures in terms of subscription revenue?",
            "Considering the filing 0000796343-24-000065, what were the latest figures in terms of subscription revenue?", """
SELECT tag, ddate, qtrs, uom, value
FROM NUM
WHERE tag = 'SubscriptionRevenue'
AND adsh='0000796343-24-000065'
AND ddate = 20240229
    """)

def query_string28():
    return ("What are the components of its non-operating income expense?",
            "Considering the filing 0001213900-24-014627, what are the components of its non-operating income expense?", """
SELECT ctag
FROM CAL
WHERE adsh='0001213900-24-014627'
AND ptag='NonoperatingIncomeExpense'
    """)

def query_string29():
    return ("Are there any credit risk factors mentioned?",
            "Are there any credit risk factors mentioned in the filing 0000950170-24-031460?", """
SELECT txt.tag, txt.version, txt.ddate, txt.qtrs, txt.iprx, txt.value, NULL AS tlabel, NULL AS doc
FROM TXT AS txt 
WHERE txt.adsh='0000950170-24-031460' 
AND txt.tag='ConcentrationRiskCreditRisk'
AND txt.version='us-gaap/2023'
AND txt.ddate=20231231
AND txt.qtrs=4
AND txt.iprx=0

UNION ALL

SELECT t.tag, t.version, NULL AS ddate, NULL AS qtrs, NULL AS iprx, NULL AS value, t.tlabel, t.doc
FROM TAG AS t
WHERE t.tag='ConcentrationRiskCreditRisk'
AND t.version='us-gaap/2023'
    """)

def query_string30():
    return ("What is the reporting date?",
            "For the filing 0001654954-24-003873, what is the reporting date?", """
SELECT period, FY, filed, accepted
FROM SUB
WHERE adsh='0001654954-24-003873' 
    """)

def query_string31():
    return ("Check if the filing has negative revenues in 2023",
            "Check if the filing 0000002178-24-000035 has negative revenues in 2023", """
SELECT ddate, value 
FROM NUM 
WHERE adsh='0000002178-24-000035' 
AND tag='Revenues' 
AND value < 0 
AND ddate BETWEEN 20230101 AND 20231231
    """)

def query_string32():
    return ("Is there an explanation provided for the tag 'TradeAndOtherAccountsReceivablePolicy' of version gaap 2023?",
            "Considering the filing 0000072903-24-000034, is there an explanation provided for the tag 'TradeAndOtherAccountsReceivablePolicy' of version gaap 2023?", """
SELECT tag, version, ddate, value 
FROM TXT
WHERE adsh='0000072903-24-000034'
AND tag='TradeAndOtherAccountsReceivablePolicy'
    """)

def query_string33():
    return ("What is the total amount DELTA has spent purchasing airline capacity in recent years?",
            "What is the total amount DELTA (adsh 0000027904-24-000003) has spent purchasing airline capacity in recent years?", """
SELECT n.adsh, n.tag, n.version, n.ddate, n.qtrs, n.uom, n.value 
FROM num n
WHERE n.adsh = '0000027904-24-000003' AND n.tag = 'AirlineCapacityPurchaseArrangements'   """)

def query_string34():
    return ("How many planes did DELTA rent in 2023 and for what cost?",
            "How many planes did DELTA (adsh 0000027904-24-000003) rent in 2023 and for what cost?", """
SELECT n.tag, n.version, n.ddate, n.uom, n.value 
FROM num n
WHERE n.adsh = '0000027904-24-000003' AND n.ddate = 20231231 AND n.tag = 'AircraftRental'

UNION ALL

SELECT n.tag, n.version, n.ddate, n.uom, n.value 
FROM num n 
WHERE n.adsh = '0000027904-24-000003' AND n.ddate = 20231231 AND n.tag = 'LeaseComponentOfPurchaseAgreementsNumberOfAircraft'

        """)

def query_string35():
    return ("How much capital does DELTA normally dedicate to aircraft repairs?",
            "How much capital does DELTA (adsh 0000027904-24-000003) normally dedicate to aircraft repairs?", """
SELECT  n.tag, n.version, n.ddate, n.uom, n.value 
FROM num n 
WHERE n.adsh = '0000027904-24-000003' AND n.tag = 'AircraftMaintenanceMaterialsAndRepairs'
    """)

def query_string36():
    return ("What was the revenue brought in by the loyalty program over the last 3 years?",
            "What was the revenue brought in by DELTA's (adsh 0000027904-24-000003) loyalty program over the last 3 years?", """
SELECT n.adsh, n.tag, n.version, n.ddate, n.qtrs, n.uom, n.value 
FROM num n JOIN sub s ON n.adsh = s.adsh 
WHERE s.adsh = '0000027904-24-000003' AND s.form = '10-K' AND s.fy = 2023 AND n.tag IN ('SalesOfMileageCredits')

UNION ALL

SELECT t.adsh, t.tag, t.version, t.ddate, t.qtrs, NULL as uom, t.value
FROM txt t
WHERE t.adsh = '0000027904-24-000003' AND t.tag = 'DisaggregationOfRevenueTableTextBlock'
        """)

def query_string37():
    return ("What have been the liabilities created by DELTA's loyalty program in 2021, 2022 and 2023?",
            "What have been the liabilities created by DELTA's (adsh 0000027904-24-000003) loyalty program in 2021, 2022 and 2023?", """
SELECT n.adsh, n.tag, n.value, n.ddate, n.uom 
FROM num n JOIN sub s ON n.adsh = s.adsh 
WHERE s.adsh = '0000027904-24-000003' AND s.form = '10-K' AND s.fy IN (2021, 2022, 2023) AND n.tag IN ('LoyaltyProgramLiabilityMileageCreditsEarned','LoyaltyProgramLiabilityTravelMileageCreditsRedeemed','LoyaltyProgramLiabilityNonTravelMileageCreditsRedeemed')
        """)

def query_string38():
    return ("How much capital did Disney report as amortization of content in 2023?",
            "How much capital did Disney report as amortization of content in 2023 (adsh 0001744489-24-000081)?", """
SELECT n.tag, n.ddate, n.qtrs, n.uom, n.value 
FROM num n 
WHERE n.adsh = '0001744489-24-000081' AND n.tag IN ('AmortizationOfProducedAndLicensedContentCostsTotal','AmortizationOfProducedContentCosts','AmortizationOfLicensedTelevisionAndProgrammingRights') AND n.ddate = 20231231
       """)

def query_string39():
    return ("How does Disney break down the costs of producing its content?",
            "How does Disney break down the costs of producing its content in its filing (adsh 0001744489-24-000081)?", """

SELECT tag, version, ddate, value
FROM TXT
WHERE adsh = '0001744489-24-000081' AND tag = 'ProducedAndLicensedContentCostsAndAdvancesDisclosureTextBlock'
       """)
def query_string40():
    return ("What is the reported value of Disney's parks and resorts in 2023?",
            "What is the reported value of Disney's parks and resorts in 2023 (adsh 0001744489-24-000081)?", """
SELECT n.tag, n.ddate, n.qtrs, n.uom, n.value 
FROM num n 
WHERE n.adsh = '0001744489-24-000081' AND n.tag = 'ParksResortsAndOtherPropertyAtCostExcludingProjectsAndLand' AND n.ddate BETWEEN 20230101 AND 20231231
       """)