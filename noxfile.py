import nox
from nox.sessions import Session


@nox.session(python="3.8", reuse_venv=True)
def tests(session: Session):
    # fmt: off
    session.run(
        "poetry", "export",
        "--dev",
        "-o", "requirements.txt",
        external=True,
    )
    # fmt: on
    session.install("-r", "requirements.txt", "-e", ".")
    session.run("pytest")
