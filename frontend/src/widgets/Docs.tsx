import overview from '../docs/0-overview.md';
import setup from '../docs/1-setup.md';
import auth from '../docs/2-auth.md';
import basics from '../docs/3-basics.md';
import data from '../docs/4-data.md';
import environment from '../docs/5-environment.md';
import whiteboard from '../docs/6-whiteboards.md';
import integrations from '../docs/7-integrations.md'
import ReactMarkdown from 'react-markdown'
import {useState} from 'react';

export function Overview() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(overview)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (<>{state}</>)
}

export function Setup() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(setup)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Auth() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(auth)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Basics() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(basics)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Environment() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(environment)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Data() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(data)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Whiteboards() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(whiteboard)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Integrations() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(integrations)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}
