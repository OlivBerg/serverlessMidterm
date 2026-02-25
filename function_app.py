# =============================================================================
# IMPORTS
# =============================================================================
import azure.functions as func            # Azure Functions SDK
import azure.durable_functions as df      # Durable Functions extension
from azure.data.tables import TableServiceClient, TableClient  # Table Storage SDK
import logging                            # Python built-in logging
import json                               # Python built-in JSON handling
import io                                 # Python built-in for byte stream handling
import os                                 # Python built-in for environment variables
import uuid                               # Python built-in for generating unique IDs
from datetime import datetime             # Python built-in for timestamps
from pypdf import PdfReader
import re

# =============================================================================
# CREATE THE DURABLE FUNCTION APP
# =============================================================================
# Same as Week 4: df.DFApp instead of func.FunctionApp
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# =============================================================================
# TABLE STORAGE HELPER
# =============================================================================
# This helper function creates a connection to Azure Table Storage.
# It uses the same connection string as our Blob trigger.
# For local development, this connects to Azurite's Table Storage emulator.
TABLE_NAME = "PDFAnalysisResults"

def get_table_client():
    """Get a TableClient for storing/retrieving analysis results."""
    connection_string = os.environ["PDFStorageConnection"]
    table_service = TableServiceClient.from_connection_string(connection_string)
    # create_table_if_not_exists ensures the table exists before we use it
    table_service.create_table_if_not_exists(TABLE_NAME)
    return table_service.get_table_client(TABLE_NAME)

# =============================================================================
# 1. CLIENT FUNCTION (Blob Trigger - The Entry Point)
# =============================================================================
# Unlike Week 4 where you used an HTTP trigger, this function triggers
# automatically when an image is uploaded to the "images" container.
#
# How it works:
#   1. User uploads an image to the "images" container in Blob Storage
#   2. Azure detects the new blob and triggers this function
#   3. This function starts the orchestrator, passing the blob name
#
# The path "images/{name}" means:
#   - Watch the "images" container
#   - {name} captures the filename (e.g., "photo.jpg")
@myApp.blob_trigger(
    arg_name="myblob",
    path="pdf/{name}",
    connection="PDFStorageConnection"
)
@myApp.durable_client_input(client_name="client")
async def blob_trigger(myblob: func.InputStream, client):
    # Get the blob name (e.g., "images/photo.jpg")
    blob_name = myblob.name
    # Read the blob content as bytes (the actual image data)
    blob_bytes = myblob.read()
    # Get the file size in KB
    blob_size_kb = round(len(blob_bytes) / 1024, 2)

    logging.info(f"New pdf file detected: {blob_name} ({blob_size_kb} KB)")

    # Prepare input data for the orchestrator
    # We pass the blob name and the raw image bytes (as a list of integers)
    # Note: We convert bytes to a list because Durable Functions serialize
    # inputs as JSON, and JSON doesn't support raw bytes
    input_data = {
        "blob_name": blob_name,
        "blob_bytes": list(blob_bytes),
        "blob_size_kb": blob_size_kb
    }

    # Start the orchestrator (same concept as Week 4's client.start_new)
    instance_id = await client.start_new(
        "pdf_analyzer_orchestrator",
        client_input=input_data
    )

    logging.info(f"Started orchestration {instance_id} for {blob_name}")

# =============================================================================
# 2. ORCHESTRATOR FUNCTION (The Workflow Manager)
# =============================================================================
# This orchestrator implements a HYBRID pattern:
#   - Fan-Out/Fan-In: Run 4 analyses in parallel
#   - Chaining: Then generate report -> store results (sequential)
#
# Compare to Week 4's orchestrator:
#   Week 4: yield call_activity(...) three times sequentially
#   Lab 2:  yield context.task_all([...]) for parallel, then yield for sequential
@myApp.orchestration_trigger(context_name="context")
def pdf_analyzer_orchestrator(context):
    # Get the input data passed from the blob trigger
    input_data = context.get_input()

    logging.info(f"Orchestrator started for: {input_data['blob_name']}")

    # =========================================================================
    # STEP 1: FAN-OUT - Run all 4 analyses in parallel
    # =========================================================================
    # Create a list of tasks WITHOUT yielding each one individually.
    # Each call_activity starts a task but doesn't wait for it.
    analysis_tasks = [
        context.call_activity("extract_text", input_data),
        context.call_activity("extract_metadata", input_data),
        context.call_activity("analyze_statistics", input_data),
        context.call_activity("detect_sensitive_data", input_data),
    ]

    # FAN-IN: yield context.task_all() waits for ALL tasks to complete.
    # This is the key difference from Week 4's sequential yield.
    # All 4 activities run simultaneously, and we get all results at once.
    results = yield context.task_all(analysis_tasks)

    # results is a list in the same order as analysis_tasks:
    # results[0] = extract text result
    # results[1] = extract metadata result
    # results[2] = analyze statistics result
    # results[3] = detect sensitive data result

    # =========================================================================
    # STEP 2: CHAIN - Generate report from combined results
    # =========================================================================
    # Now we chain: take the parallel results and combine them into a report.
    # This must happen AFTER all analyses complete (sequential).
    report_input = {
        "blob_name": input_data["blob_name"],
        "text": results[0],
        "metadata": results[1],
        "statistics": results[2],
        "sensitive_data": results[3],
    }
    report = yield context.call_activity("generate_report", report_input)

    # =========================================================================
    # STEP 3: CHAIN - Store the report in Table Storage
    # =========================================================================
    # Final step: persist the report to Azure Table Storage.
    record = yield context.call_activity("store_results", report)
    return record

# ACTIVITY: extract Text using py
@myApp.activity_trigger(input_name="inputData")
def extract_text(inputData: dict):
    logging.info("Extracting text...")
    try:
        pdf_bytes = bytes(inputData["blob_bytes"])
        reader = PdfReader(io.BytesIO(pdf_bytes))

        all_text = []
        for page in reader.pages:
            text = page.extract_text() or ""
            if text:
                all_text.append(text)

        return {
            "hasText": False if len(all_text) == 0 else True,
            "extractedText": "\n".join(all_text),
            "confidence": 0.0,
            "language": "unknown",
        }

    except Exception as e:
        logging.error(f"extract text failed: {str(e)}")
        return {
            "hasText": False,
            "extractedText": "",
            "confidence": 0.0,
            "error": str(e)
        }

# ACTIVITY: Extract Metadata (Real Analysis)
@myApp.activity_trigger(input_name="inputData")
def extract_metadata(inputData: dict):
    logging.info("Extracting metadata...")
    try:
        pdf_bytes = bytes(inputData["blob_bytes"])
        reader = PdfReader(io.BytesIO(pdf_bytes))
        metadata = reader.metadata
        return {
            "title": metadata.title,
            "author": metadata.author,
            "creator": metadata.creator,
            "producer": metadata.producer,
            "creation_date": metadata.creation_date.isoformat(),
            "mod_date": metadata.modification_date.isoformat(),
        }

    except Exception as e:
        logging.error(f"Metadata analysis failed: {str(e)}")
        return {
            "title": "",
            "author": "",
            "error": str(e)
        }


# ACTIVITY: Analyze statistics (Real Analysis)
@myApp.activity_trigger(input_name="inputData")
def analyze_statistics(inputData: dict):
    logging.info("Analyzing statistics...")
    try:
        pdf_bytes = bytes(inputData["blob_bytes"])
        reader = PdfReader(io.BytesIO(pdf_bytes))
        page_count = len(reader.pages)

        all_text = ""
        for page in reader.pages:
            all_text += (page.extract_text() or "") + "\n"

        words = all_text.split()
        word_count = len(words)
        avg_words_per_page = word_count / page_count if page_count else 0
        reading_time_minutes = word_count / 200  # 200 wpm

        return {
            "page_count": page_count,
            "word_count": word_count,
            "avg_words_per_page": avg_words_per_page,
            "estimated_reading_time_min": reading_time_minutes,
        }

    except Exception as e:
        logging.error(f"statistics analysis failed: {str(e)}")
        return {
            "page_count": 0,
            "word_count": 0,
            "avg_words_per_page": 0,
            "estimated_reading_time_min": 0,
            "error": str(e)
        }

# ACTIVITY: Analyze statistics (Real Analysis)
@myApp.activity_trigger(input_name="inputData")
def detect_sensitive_data(inputData: dict):
    logging.info("detecting sensitive data...")
    try:
        pdf_bytes = bytes(inputData["blob_bytes"])
        reader = PdfReader(io.BytesIO(pdf_bytes))

        text = ""
        for page in reader.pages:
            text += (page.extract_text() or "") + "\n"

        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)

        # Phone numbers
        phones = re.findall(r"""
                (?:
                    \+?\d{1,3}[\s\-\.]?
                )?
                (?:\(?\d{3}\)?[\s\-\.]?)
                \d{3}[\s\-\.]?\d{4}
            """, text, re.VERBOSE)

        # URLs
        urls = re.findall(r"(https?://[^\s]+|www\.[^\s]+)", text)

        # Dates
        dates = re.findall(r"""
                (
                    \b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b |          # 2024-01-15
                    \b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b |          # 15/01/2024
                    \b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b
                )
            """, text, re.VERBOSE | re.IGNORECASE)

        return {
            "emails": emails,
            "phones": phones,
            "urls": urls,
            "dates": dates,
        }

    except Exception as e:
        logging.error(f"detecting sensitive data failed: {str(e)}")
        return {
            "emails": [],
            "phones": [],
            "urls": [],
            "dates": [],
            "error": str(e)
        }

# =============================================================================
# 7. ACTIVITY: Generate Report
# =============================================================================
# This activity takes the results from all 4 analyses and combines them
# into a single unified report. This is the "reduce" step after the fan-in.
@myApp.activity_trigger(input_name="reportData")
def generate_report(reportData: dict):
    logging.info("Generating combined report...")

    blob_name = reportData["blob_name"]
    # Extract just the filename from the full path (e.g., "images/photo.jpg" -> "photo.jpg")
    filename = blob_name.split("/")[-1] if "/" in blob_name else blob_name

    report = {
        "id": str(uuid.uuid4()),
        "fileName": filename,
        "blobPath": blob_name,
        "analyzedAt": datetime.utcnow().isoformat(),
        "analyses": {
            "text": reportData["text"],
            "metadata": reportData["metadata"],
            "statistics": reportData["statistics"],
            "sensitive_data": reportData["sensitive_data"],
        },
        "summary": {
            "format": reportData["metadata"].get("format", "Unknown"),
            "hasText": reportData["text"].get("hasText", False),
        }
    }

    logging.info(f"Report generated: {report['id']}")
    return report

# =============================================================================
# 8. ACTIVITY: Store Results in Table Storage
# =============================================================================
# This activity saves the generated report to Azure Table Storage.
#
# Table Storage requires two keys:
#   - PartitionKey: Groups related entities (we use "ImageAnalysis")
#   - RowKey: Unique identifier within the partition (we use the report ID)
@myApp.activity_trigger(input_name="report")
def store_results(report: dict):
    logging.info(f"Storing results for {report['fileName']}...")

    try:
        table_client = get_table_client()

        # Table Storage entities are flat key-value pairs.
        # Complex nested data (like our analyses) must be serialized as JSON strings.
        entity = {
            "PartitionKey": "PDFAnalysis",
            "RowKey": report["id"],
            "FileName": report["fileName"],
            "BlobPath": report["blobPath"],
            "AnalyzedAt": report["analyzedAt"],
            # Store complex data as JSON strings
            "Summary": json.dumps(report["summary"]),
            "TextAnalysis": json.dumps(report["analyses"]["text"]),
            "MetadataAnalysis": json.dumps(report["analyses"]["metadata"]),
        }

        table_client.upsert_entity(entity)

        logging.info(f"Results stored with ID: {report['id']}")

        return {
            "id": report["id"],
            "fileName": report["fileName"],
            "status": "stored",
            "analyzedAt": report["analyzedAt"],
            "summary": report["summary"]
        }

    except Exception as e:
        logging.error(f"Failed to store results: {str(e)}")
        return {
            "id": report.get("id", "unknown"),
            "status": "error",
            "error": str(e)
        }

# =============================================================================
# 9. HTTP FUNCTION: Get Analysis Results
# =============================================================================
# This is a regular HTTP function (like Week 2) that retrieves stored results
# from Table Storage. It's NOT part of the orchestration - it's a separate
# endpoint for users to query past analyses.
#
# Usage:
#   GET /api/results          - Get all results (last 10)
#   GET /api/results/{id}     - Get a specific result by ID
@myApp.route(route="results/{id?}")
def get_results(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Get results endpoint called")

    try:
        table_client = get_table_client()
        result_id = req.route_params.get("id")

        if result_id:
            # Get a specific result by ID
            try:
                entity = table_client.get_entity(
                    partition_key="PDFAnalysis",
                    row_key=result_id
                )
                # Parse JSON strings back into objects
                result = {
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "blobPath": entity["BlobPath"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                    "analyses": {
                        "text": json.loads(entity["TextAnalysis"]),
                        "metadata": json.loads(entity["MetadataAnalysis"]),
                    }
                }

                return func.HttpResponse(
                    json.dumps(result, indent=2),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception:
                return func.HttpResponse(
                    json.dumps({"error": f"Result not found: {result_id}"}),
                    mimetype="application/json",
                    status_code=404
                )
        else:
            # Get all results (with optional limit)
            limit = int(req.params.get("limit", "10"))

            entities = table_client.query_entities(
                query_filter="PartitionKey eq 'PDFAnalysis'"
            )
            results = []
            for entity in entities:
                results.append({
                    "id": entity["RowKey"],
                    "fileName": entity["FileName"],
                    "analyzedAt": entity["AnalyzedAt"],
                    "summary": json.loads(entity["Summary"]),
                })

            # Sort by analyzedAt descending (most recent first)
            results.sort(key=lambda x: x["analyzedAt"], reverse=True)
            results = results[:limit]

            return func.HttpResponse(
                json.dumps({"count": len(results), "results": results}, indent=2),
                mimetype="application/json",
                status_code=200
            )

    except Exception as e:
        logging.error(f"Failed to retrieve results: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )