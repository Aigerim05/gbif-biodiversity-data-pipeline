
from pathlib import Path
import tempfile
import pandas as pd
import numpy as np
from src.api_client import select_fields, strata_for
from src.validator import validate, run_invalid_tests
from src.cleaner import clean
from src.loader import load_twice
from src.analysis import analyze


def samples(n=12):
    out=[]
    for i in range(n):
        year=2016 + (i%3)*3
        out.append({"key":1000+i,"scientificName":"  Test   species ",
          "kingdom":"Animalia" if i%2 else "Plantae",
          "basisOfRecord":"HUMAN_OBSERVATION" if i%2 else "PRESERVED_SPECIMEN",
          "eventDate":f"{year}-05-15", "year":float(year),
          "decimalLatitude":43.23+i/10,"decimalLongitude":76.89+i/10,
          "countryCode":"KZ","datasetKey":"synthetic-tests-only"})
    return out


def test_sampling_quota():
    strata=strata_for(301)
    assert len(strata)==6 and sum(x[3] for x in strata)==301
    assert len({(x[0],x[1],x[2]) for x in strata})==6


def test_selected_fields():
    assert len(select_fields(samples(1)[0]))==10


def test_validator_and_invalid_records():
    with tempfile.TemporaryDirectory() as tmp:
        valid, invalid, failures=validate(samples(),tmp)
        assert len(valid)==12 and len(invalid)==0
        result=run_invalid_tests(valid,tmp)
        assert result['detected_invalid']==3
        assert (Path(tmp)/'test_validation_errors.csv').exists()


def test_invalid_coordinate():
    with tempfile.TemporaryDirectory() as tmp:
        rows=samples(); rows[1]['decimalLongitude']=250
        good,bad,failures=validate(rows,tmp)
        assert len(good)==11 and len(bad)==1 and len(failures)>0


def test_clean_database_and_analysis():
    with tempfile.TemporaryDirectory() as tmp:
        good,_,_=validate(samples(),tmp)
        cleaned,stats,groups,pivot=clean(good)
        assert len(cleaned)==12 and stats['pivot_shape'][0]==3
        assert np.isfinite(cleaned['distance_to_almaty_km']).all()
        batch,result=load_twice(cleaned,str(Path(tmp)/'sample.db'))
        assert result['first_load_total']==result['second_load_total']==12
        summary=analyze(batch,str(Path(tmp)/'figures'),stats['missing_before'])
        assert len(summary['figure_interpretations'])==3
        assert len(list((Path(tmp)/'figures').glob('*.png')))==3
