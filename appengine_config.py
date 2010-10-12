def webapp_add_wsgi_middleware(app):
  try:
    from google.appengine.ext.appstats import recording
  except ImportError, err:
    logging.info('Failed to import recording: %s', err)
  else:
    app = recording.appstats_wsgi_middleware(app)
  return app
