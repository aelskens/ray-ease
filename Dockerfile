ARG IMAGE=python:3.10.12

FROM $IMAGE

LABEL maintainer="Arthur Elskens <arthur.elskens@ulb.be>"
LABEL description="The Dockerfile to build an image used for developping the ray-ease Python package."

ENV DEBIAN_FRONTEND=noninteractive

ARG USERNAME=ray
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create user that correspond to the user on the OS
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
	&& usermod --shell /bin/bash $USERNAME \
    #
    # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME
ENV USER_HOME="/home/${USERNAME}"

# Fix ownership issue with the home directory
RUN sudo chown -R ${USERNAME} ${USER_HOME}

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Get build dependencies
RUN sudo apt-get update \
	&& sudo apt-get install -y \
		build-essential \
		software-properties-common \
        git \
		ca-certificates

# Create and activate a venv
ENV VIRTUAL_ENV="${USER_HOME}/opt/ray-ease"
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Ensure the installation of all additional packages
COPY requirements.txt /tmp/packages/
COPY dev-requirements.txt /tmp/packages/

RUN pip install -r /tmp/packages/requirements.txt
RUN pip install -r /tmp/packages/dev-requirements.txt


# Clean up
RUN sudo rm -r /tmp/packages