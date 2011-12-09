def webapp_add_wsgi_middleware(app):
  try:
    from google.appengine.ext.appstats import recording
  except ImportError, err:
    logging.info('Failed to import recording: %s', err)
  else:
    app = recording.appstats_wsgi_middleware(app)
  return app

appstats_KEY_DISTANCE = 10
appstats_MAX_REPR = 1000
appstats_MAX_STACK = 20

appstats_FILTER_LIST = [
  {'PATH_INFO': '!^/favicon\.ico$'},
  ]
