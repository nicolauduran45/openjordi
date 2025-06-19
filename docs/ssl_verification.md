# SSL Verification in OpenJordi

Some data sources may have SSL certificate issues that prevent direct downloading. OpenJordi provides functionality to manage these cases effectively.

## Understanding SSL Verification Issues

SSL (Secure Sockets Layer) verification ensures that connections to websites are secure and that the server you're connecting to is legitimate. However, you might encounter these common issues:

1. **Self-signed certificates**: Some organizations use certificates they've created themselves rather than ones from trusted certificate authorities.
2. **Expired certificates**: The SSL certificate may have expired.
3. **Certificate chain issues**: The server doesn't provide the complete certificate chain needed for verification.
4. **Regional certificate authorities**: Some certificates are issued by authorities not recognized by all systems globally.

## Managing SSL Verification in OpenJordi

### 1. Setting up sources with SSL verification bypass

In your sources.csv file, add a column called "Skip SSL Verify" with values:
- `true` for sources that should bypass SSL verification
- `false` for sources that should maintain SSL verification (default)

Example:
```
Funder,Source_name,Link to dump,Format,Skip SSL Verify
Irish Research Council,Awardees,https://research.ie/awardees_search/download.php,csv,false
FAPESP,Auxílios em andamento,https://media.fapesp.br/bv/uploads/auxilios_em_andamento.csv,csv,true
```

### 2. Importing Sources with SSL Settings

Import your sources with the updated CSV:

```bash
python scripts/import_sources.py data/sources.csv
```

The script will automatically recognize the "Skip SSL Verify" column and add the appropriate setting to each source.

### 3. Manually Setting SSL Verification Flag

You can also manually add the `skip_ssl_verify` flag to sources in your `config/sources.py` file:

```python
"Some_Source": {
    "funder": "Some Funder",
    "source_name": "Dataset Name",
    "data_link": "https://example.org/download.csv",
    "format": "csv",
    "skip_ssl_verify": true  # Add this line to bypass SSL verification
},
```

## Security Considerations

Disabling SSL verification reduces security by removing validation that you're connecting to the legitimate server. Only disable verification for:

1. **Trusted sources**: Organizations you know and trust
2. **Non-sensitive data**: Public datasets where data integrity is less critical
3. **When necessary**: Try other solutions first (like updating certificates locally)

## Alternative Solutions

Instead of disabling SSL verification:

1. **Install certificates**: Add the source's certificate to your trusted store
2. **Use a proxy**: Route requests through a service that handles the verification
3. **Contact the source**: Notify them of the SSL issues so they can fix them
4. **Manual downloads**: Download files manually and place them in your data directory

## Implementation Details

The current implementation includes:

1. Automatic fallback: If a download fails with SSL errors, it attempts one final try with verification disabled
2. Selective application: SSL verification is only disabled for sources with the `skip_ssl_verify` flag
3. Logging: When SSL verification is disabled, a warning is logged for audit purposes

This approach provides a flexible solution while maintaining security for most sources.