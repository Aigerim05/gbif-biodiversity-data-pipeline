
from pathlib import Path
import json
from xml.sax.saxutils import escape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether, Preformatted
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

DARK = colors.HexColor('#20354A')
BLUE = colors.HexColor('#E8F0F5')


def create_report(metrics, output='report/project_report.pdf'):
    if metrics is None:
        raise ValueError('A report can only be generated from executed pipeline metrics')
    out = Path(output); out.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='TtlV2', parent=styles['Title'], fontName='Helvetica-Bold',
                              fontSize=22, leading=27, textColor=DARK, spaceAfter=20))
    styles.add(ParagraphStyle(name='SecV2', parent=styles['Heading2'], fontSize=13,
                              leading=17, textColor=DARK, spaceBefore=15, spaceAfter=7,
                              keepWithNext=True))
    styles.add(ParagraphStyle(name='TextV2', parent=styles['Normal'], fontSize=9.2,
                              leading=13.5, spaceAfter=6))
    styles.add(ParagraphStyle(name='SmallV2', parent=styles['Normal'], fontSize=8,
                              leading=11, spaceAfter=4))
    styles.add(ParagraphStyle(name='MonoV2', fontName='Courier', fontSize=7, leading=9))
    story=[]
    def para(txt, style='TextV2'):
        story.append(Paragraph(escape(str(txt)), styles[style]))
    def head(n,name): story.append(Paragraph(f'{n}. {escape(name)}',styles['SecV2']))
    def table(headers, rows, widths=None):
        payload=[[Paragraph(escape(str(c)),styles['SmallV2']) for c in headers]]
        payload.extend([[Paragraph(escape(str(c)),styles['SmallV2']) for c in row] for row in rows])
        if not rows: return
        if widths is None: widths=[480/len(headers)]*len(headers)
        tb=Table(payload,colWidths=widths,repeatRows=1,hAlign='LEFT')
        tb.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),BLUE),('TEXTCOLOR',(0,0),(-1,0),DARK),
          ('GRID',(0,0),(-1,-1),.3,colors.HexColor('#D0DDE5')),
          ('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),6),
          ('RIGHTPADDING',(0,0),(-1,-1),6),('TOPPADDING',(0,0),(-1,-1),5),
          ('BOTTOMPADDING',(0,0),(-1,-1),4)]))
        story.append(tb); story.append(Spacer(1,7))
    def code(s): story.append(Preformatted(s,styles['MonoV2']))
    def figure(filename,caption):
        p=Path('figures')/filename
        if p.is_file():
            im=Image(str(p),width=440,height=250,kind='proportional')
            story.append(KeepTogether([im, Paragraph(escape(caption),styles['SmallV2'])]))
    para('Biodiversity Observation Data Quality and Analysis Pipeline','TtlV2')
    para('Course: Data Collection & Preparation — SIS2 (Apache Kafka and Pandera)')
    para('This is an automatically generated technical results report. See report/DC_SIS_2_Report.pdf for the submitted report and group details.')
    para(f'Executed run ID: {metrics["run_id"]}; collection UTC: {metrics["collection"].get("collected_at_utc","unknown")}')
    para('Evidence status: pipeline metrics and visualizations were generated from the current execution. Screenshots from local Docker/terminal should be attached separately before submission.')
    head(2,'Introduction')
    para('GBIF occurrence records are observations or other evidence of the presence of organisms. This project studies the data engineering and data quality of a small, explicitly stratified sample of Kazakhstan GBIF records. It does not estimate organism abundance or national species richness.')
    para('Objectives: retrieve authentic public data; transfer all records through a real Kafka broker; validate before cleaning; quarantine invalid records; transform and load into a relational SQLite database; examine categorical, temporal and geographic dimensions; document reliability and idempotency.')
    para('Research questions: (1) Which kingdoms are represented within each sampled period? (2) How are observed locations distributed by kingdom? (3) How does distance from Almaty differ between human observations and preserved specimens? (4) Which fields are missing or fail validation?')
    head(3,'Data Source and Collection')
    m=metrics['collection']
    table(['Field','Observed configuration'],[
      ['Endpoint',metrics['api_endpoint']], ['Country', 'KZ'],['Coordinate filter','hasCoordinate=true'],
      ['Method','Six fixed-quota strata: three periods times two record bases'],
      ['Request settings','year, basisOfRecord, country, hasCoordinate, limit, offset'],
      ['Raw preservation','data/raw_data.json contains original GBIF record objects before modification'],
      ['Collection time (UTC)',m.get('collected_at_utc','n/a')],
      ['Unique records collected',metrics['api_collected']]], [140,340])
    table(['Period','Basis','Count'],[[s['period'],s['basis'],s['obtained']] for s in m['strata']], [120,230,130])
    para('Numerical fields: key, year, decimalLatitude, decimalLongitude. Categorical fields: kingdom, basisOfRecord, countryCode, datasetKey. Temporal field: eventDate. JSON records are preserved in full; a documented subset of ten fields is streamed. Source API example:')
    raw=Path('data/raw_data.json')
    if raw.is_file():
        example=json.loads(raw.read_text(encoding='utf-8'))[0]
        code(json.dumps({k:example.get(k) for k in ['key','scientificName','kingdom','basisOfRecord','eventDate','decimalLatitude','decimalLongitude']},indent=2,ensure_ascii=True)[:600])
    para('Sampling caveat: the six strata have fixed planned quotas, so relative counts of periods and bases are predetermined by the data collection plan. Comparisons within strata describe only this convenience sample. They do not represent natural prevalence or temporal trends.')
    head(4,'Pipeline Architecture')
    para('GBIF REST API -> raw JSON + manifest -> producer.py (JSON messages, run_id) -> real Kafka topic -> consumer.py -> JSONL -> Pandera schema -> quarantine / accepted CSV -> pandas & NumPy -> SQLite3 -> SQL -> EDA PNG/CSV -> generated PDF. main.py orchestrates all stages; no parallel synthetic input is used.')
    para('The source manifest records collection parameters and quotas. Every Kafka message belongs to a run_id. The consumer stops after the expected number of messages or raises on timeout. Old messages from earlier runs are excluded.')
    head(5,'Apache Kafka Implementation')
    table(['Element','Implementation'],[
      ['Broker','apache/kafka:4.1.2, KRaft, advertised localhost:9092, Docker Compose'],
      ['Topic','gbif_observations, one partition, replication factor 1'],
      ['Producer','src/producer.py: JSON serialization, acks=all, blocking acknowledgement per message'],
      ['Consumer','src/consumer.py: JSON decoding, per-run consumer group, run_id filtering'],
      ['Termination','Target matching message count or explicit timeout'],
      ['Error handling','HTTP retries; Kafka connection, send, malformed payload and timeout exceptions'],
      ['Published / consumed',f'{metrics["kafka_published"]} / {metrics["kafka_consumed"]}']],[135,345])
    code('producer.send(topic, value=message).get(timeout=30)\n# Consumer reads JSON messages and checks message run_id')
    para('Kafka evidence is recorded as completed acknowledged sends and matching Consumer messages. Actual terminal screenshots may be added to the separately submitted report.')
    head(6,'Pandera Validation')
    para('Rows are checked before cleaning. Schema rules: positive non-null unique integer key; required species name length >=2; allowed observation basis; latitude [-90,90]; longitude [-180,180]; countryCode KZ; optional year in [1600,2026]; optional parseable date. Rejected rows and error details are saved to CSV, not loaded.')
    table(['Result','Actual count'],[['Valid authentic records',metrics['valid']],
          ['Invalid authentic records',metrics['invalid']],['Validation failures',metrics['validation_failures']],
          ['Deliberately invalid tests',metrics['invalid_tests']['deliberate_test_count']],
          ['Deliberate records rejected',metrics['invalid_tests']['detected_invalid']]], [270,210])
    errors_file=Path('data/test_validation_errors.csv')
    if errors_file.is_file():
        import pandas as pd
        failures=pd.read_csv(errors_file).fillna('')
        example_rows=[]
        for _,r in failures.head(9).iterrows():
            example_rows.append([r.get('index',''),r.get('column',''),str(r.get('failure_case',''))[:40],str(r.get('check',''))[:50]])
        if example_rows: table(['Test row','Column','Failure','Check'],example_rows,[65,135,125,155])
    para('Three test records deliberately break latitude, year and observation basis. These test cases never pass through the production Kafka stream and never enter SQLite.')
    head(7,'Data Cleaning and Transformation')
    c=metrics['cleaning']
    para('Only Pandera-accepted records are transformed. Duplicate GBIF keys are removed; optional kingdom is explicitly labeled Unknown; scientific names are trimmed and inner whitespace standardized; date/year converted to analysis types; missing year is derived from parseable eventDate if available. No values are fabricated to force nonzero cleaning changes.')
    para('NumPy uses the Haversine formula to measure great-circle distance in km between each GBIF location and the fixed Almaty reference (43.238949N, 76.889709E). Distances are geographic descriptions, not ecological abundance. A two-condition filter selects human observations within 500 km of Almaty without discarding valid stored rows. GroupBy summarizes period x record basis; pivot_table reshapes period x kingdom counts.')
    table(['Metric','Observed'],[['Accepted before cleaning',c['before_cleaning']],
      ['After cleaning',c['after_cleaning']],['Duplicates removed',c['duplicates_removed']],
      ['Filtered analysis subset',c['filtered_analysis_count']],
      ['Pivot dimension',str(c['pivot_shape'])],
      ['Median distance from Almaty (km)',c['median_distance_to_almaty_km']]], [310,170])
    table(['Field','Missing before','Missing after'],[[k,v,c['missing_after'].get(k,'--')]
      for k,v in c['missing_before'].items()], [220,130,130])
    head(8,'SQLite3 Database Implementation')
    d=metrics['database']
    para('src/loader.py defines an occurrences table with INTEGER PRIMARY KEY(key), typed fields and CHECK constraints for coordinates and country_code. Existing keys are compared field by field; identical records are safely reused and changed records raise an error without modifying stored data. The same batch is loaded twice to verify that row counts and content stay unchanged. All batch keys are verified with SELECT. V2 uses data/output_v2.db to avoid the old schema.')
    code('SELECT COUNT(*) AS total FROM occurrences;\nSELECT COUNT(DISTINCT key) AS unique_keys FROM occurrences;\nSELECT basis_of_record, COUNT(*) AS n\nFROM occurrences GROUP BY basis_of_record ORDER BY n DESC;')
    table(['SQL test','Observed'],[['Current processed batch',d['processed_records']],
       ['DB after first insert',d['first_load_total']],['DB after second insert',d['second_load_total']],
       ['Unique DB identifiers',d['unique_database_keys']],
       ['Current batch present',d['database_contains_current_batch']],
       ['Idempotency',d['unchanged_on_second_load']]], [290,190])
    para(f'Example SQL GROUP BY result: {d["sql_verification"]["basis_result"]}')
    head(9,'Exploratory Data Analysis')
    a=metrics['analysis']
    para(f'Exactly {a["database_records_analyzed"]} current-batch rows were read back from SQLite for EDA. Kingdom counts: {a["kingdom_counts"]}. Period counts: {a["period_counts"]}. Basis counts: {a["basis_counts"]}. Numeric summaries are in figures/descriptive_statistics.csv; group and pivot outputs are in figures/*.csv.')
    for i,(filename,interpretation) in enumerate(a['figure_interpretations'].items(),1):
        figure(filename,f'Figure {i}. {interpretation}')
    head(10,'Pipeline Testing and Verification')
    table(['Stage','Records'],[['GBIF collected',metrics['api_collected']],['Kafka acknowledged',metrics['kafka_published']],
      ['Kafka consumed',metrics['kafka_consumed']],['Valid before cleaning',metrics['valid']],
      ['Invalid authentic rows',metrics['invalid']],['After cleaning',c['after_cleaning']],
      ['Stored in current SQLite batch',d['current_batch_stored']]], [280,200])
    para('Checks: API count = acknowledged sends = consumed count; accepted + rejected = consumed; cleaned <= accepted; current batch keys exist in database; repeated loading leaves row count unchanged. The three invalid test cases are distinct from the authentic dataset and are intentionally excluded. Offline pytest tests and live Kafka run are different kinds of evidence.')
    para('Previous execution issues can include inactive Docker daemon, addressed by starting Docker Desktop. Real message transfer was verified via the current run counts. No claim is made that these runs prove reliability under concurrent broker failure.')
    head(11,'Conclusions')
    para(f'The completed pipeline transferred {metrics["kafka_published"]} real GBIF observations through Kafka, validated {metrics["valid"]} rows, quarantined {metrics["invalid"]} authentic invalid rows and retained {d["current_batch_stored"]} current-batch records in SQLite. Three deliberately invalid records were rejected in a separate test.')
    para(f'Observed descriptive results: {a["kingdom_counts"]}; median Haversine distance to Almaty by basis: {a["median_distance_by_basis_km"]}. The analysis shows the characteristics of this selected dataset, not national biodiversity abundance.')
    para('Limitations: fixed period/basis quotas, source reporting bias, missing optional fields, possible geographic clustering, and changing GBIF API results over time. The saved raw JSON and manifest enable replay with --reuse, but live GBIF pulls may differ.')
    head(12,'Individual Contributions')
    para('Individual contributions are documented in the separately submitted report (report/DC_SIS_2_Report.pdf); this automated metrics report does not infer contributor identities.')
    head(13,'References')
    for line in ['GBIF Occurrence API: https://techdocs.gbif.org/en/openapi/v1/occurrence',
      'Apache Kafka Docker: https://kafka.apache.org/41/getting-started/docker/',
      'Pandera: https://pandera.readthedocs.io/', 'pandas: https://pandas.pydata.org/docs/',
      'NumPy: https://numpy.org/doc/', 'Python SQLite: https://docs.python.org/3/library/sqlite3.html',
      'Matplotlib: https://matplotlib.org/stable/']:
        para(line,'SmallV2')
    para('Before final submission, ensure real Docker/Kafka and SQL verification screenshots are included in report/DC_SIS_2_Report.pdf.')
    def foot(can,doc):
        can.setFont('Helvetica',8); can.setFillColor(colors.grey)
        can.drawString(54,32,'SIS2 | GBIF data streaming and validation')
        can.drawRightString(560,32,f'Page {doc.page}')
    doc=SimpleDocTemplate(str(out),pagesize=(612,792),leftMargin=60,rightMargin=60,
        topMargin=48,bottomMargin=47,title='GBIF SIS2 research and implementation report')
    doc.build(story,onFirstPage=foot,onLaterPages=foot)
    return out

if __name__=='__main__':
    path=Path('data/metrics.json')
    if not path.is_file(): raise SystemExit('Run python main.py --count 300 first')
    print(create_report(json.loads(path.read_text(encoding='utf-8'))))
