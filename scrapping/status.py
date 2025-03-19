from http import HTTPStatus


redirects = {HTTPStatus.MOVED_PERMANENTLY,
             HTTPStatus.FOUND,
             HTTPStatus.TEMPORARY_REDIRECT,
             HTTPStatus.PERMANENT_REDIRECT
             }
