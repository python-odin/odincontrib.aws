import nox
from nox.sessions import Session


@nox.session(python="3.10", reuse_venv=True)
def tests(session: Session):
    # fmt: off
    session.run(
        "poetry", "export",
        "--dev", "--without-hashes",
        "-o", "requirements.txt",
        external=True,
    )
    # fmt: on
    session.install("-r", "requirements.txt", "-e", ".")
    session.run("pytest")
