AC_INIT(clopinet,0.1)
m4_include([m4/ocaml.m4])

AC_PROG_CC

AC_PROG_OCAML
if test "$OCAMLC" = "no"; then
  AC_MSG_ERROR([You must install the OCaml compiler])
fi

AC_PROG_FINDLIB
if test "$OCAMLFIND" = "no"; then
  AC_MSG_ERROR([You must install OCaml findlib (the ocamlfind command)])
fi

## OCaml installs graphics/META even without graphics support :-(
#AC_CHECK_OCAML_PKG([graphics])
#if test "$OCAML_PKG_graphics" = "no"; then
#  AC_WARN([Graphics library not found.])
#fi

AC_CHECK_OCAML_MODULE(have_graphics,graphics,[Graphics],[+graphics])
if test -z "$have_graphics" ; then
  AC_WARN([Graphics library not found.])
fi

AC_CHECK_OCAML_WORD_SIZE

AC_ARG_VAR(GEOIPDB, [Location of the GeoIP database to use])
AC_ARG_VAR(DBDIR, [Location of database])
AC_ARG_VAR(ADD_PATH, [Additional directories to add into PATH for compiling])

AC_CONFIG_FILES([Makefile make.common])
AC_OUTPUT
